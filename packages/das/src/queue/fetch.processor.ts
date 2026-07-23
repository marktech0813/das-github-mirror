import { Processor, WorkerHost, InjectQueue } from "@nestjs/bullmq";
import { Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import { QueryDeepPartialEntity } from "typeorm/query-builder/QueryPartialEntity";
import { DelayedError, Job, Queue } from "bullmq";
import { Issue, PullRequest } from "../entities";
import { GitHubFetcherService } from "../webhook/github-fetcher.service";
import { GitHubRateLimitError } from "../webhook/github-rate-limit.error";
import {
  FETCH_QUEUE,
  FETCH_JOBS,
  DEFAULT_BACKFILL_DAYS,
  prFilesJobId,
} from "./constants";

export interface PrMetadataJobData {
  repoFullName: string;
  prNumber: number;
}

export interface PrFilesJobData {
  repoFullName: string;
  prNumber: number;
  expectedHeadSha?: string | null;
  expectedBaseSha?: string | null;
}

export interface BackfillRepoJobData {
  repoFullName: string;
  days?: number;
}

export interface IssueClosureJobData {
  repoFullName: string;
  issueNumber: number;
}

interface PrFilesGeneration {
  headSha: string | null;
  baseSha: string | null;
}

type JobData =
  | PrMetadataJobData
  | PrFilesJobData
  | BackfillRepoJobData
  | IssueClosureJobData;

@Processor(FETCH_QUEUE, { concurrency: 5 })
export class FetchProcessor extends WorkerHost {
  private readonly logger = new Logger(FetchProcessor.name);

  constructor(
    private readonly fetcher: GitHubFetcherService,
    @InjectRepository(PullRequest)
    private readonly prRepo: Repository<PullRequest>,
    @InjectRepository(Issue)
    private readonly issueRepo: Repository<Issue>,
    @InjectQueue(FETCH_QUEUE)
    private readonly fetchQueue: Queue,
  ) {
    super();
  }

  async process(job: Job<JobData>, token?: string): Promise<void> {
    try {
      switch (job.name) {
        case FETCH_JOBS.PR_METADATA: {
          const { repoFullName, prNumber } = job.data as PrMetadataJobData;
          await this.handlePrMetadata(repoFullName, prNumber);
          break;
        }
        case FETCH_JOBS.PR_FILES: {
          await this.handlePrFiles(job.data as PrFilesJobData);
          break;
        }
        case FETCH_JOBS.BACKFILL_REPO: {
          const { repoFullName, days } = job.data as BackfillRepoJobData;
          await this.handleBackfill(
            repoFullName,
            days ?? DEFAULT_BACKFILL_DAYS,
          );
          break;
        }
        case FETCH_JOBS.ISSUE_CLOSURE: {
          const { repoFullName, issueNumber } = job.data as IssueClosureJobData;
          await this.handleIssueClosure(repoFullName, issueNumber);
          break;
        }
        default:
          this.logger.warn(`Unknown job name: ${job.name}`);
      }
    } catch (err) {
      // GitHub budget exhausted: defer this job until the limit resets instead
      // of failing it. moveToDelayed re-queues without consuming a retry attempt
      // and frees the worker slot for other jobs (e.g. other installations,
      // which have independent budgets) while we wait.
      if (err instanceof GitHubRateLimitError) {
        const retryAt = Date.now() + err.retryAfterMs;
        this.logger.warn(
          `[rate-limit-defer] job=${job.name} id=${job.id} ` +
            `retry_in_s=${Math.round(err.retryAfterMs / 1000)}`,
        );
        await job.moveToDelayed(retryAt, token);
        throw new DelayedError();
      }
      throw err;
    }
  }

  private async handleIssueClosure(
    repoFullName: string,
    issueNumber: number,
  ): Promise<void> {
    this.logger.log(`Resolving closer for ${repoFullName}#${issueNumber}`);

    const issue = await this.issueRepo.findOneBy({
      repoFullName,
      issueNumber,
    });
    if (!issue) return;

    // Reopens already null out solvedByPr in the webhook handler; never
    // re-fetch for an open issue.
    if (issue.state !== "CLOSED") {
      await this.issueRepo.update(
        { repoFullName, issueNumber },
        { solvedByPr: null },
      );
      return;
    }

    const solvedByPr = await this.fetcher.fetchIssueClosingPr(
      repoFullName,
      issueNumber,
    );

    // Re-check state at write time. A reopen can land during the GraphQL fetch
    // after the early CLOSED check above; writing solved_by_pr onto an OPEN
    // issue would corrupt issue-discovery scoring (#199).
    await this.issueRepo.update(
      { repoFullName, issueNumber, state: "CLOSED" },
      { solvedByPr },
    );
  }

  private async handlePrMetadata(
    repoFullName: string,
    prNumber: number,
  ): Promise<void> {
    this.logger.log(`Fetching PR metadata for ${repoFullName}#${prNumber}`);

    const {
      closingIssueNumbers,
      body,
      lastEditedAt,
      state,
      mergedAt,
      closedAt,
      mergedByLogin,
    } = await this.fetcher.fetchPrMetadata(repoFullName, prNumber);
    const currentClosingIssueNumbers =
      this.uniqueIssueNumbers(closingIssueNumbers);

    // Re-assert authoritative state from GraphQL so a missed
    // `pull_request.closed` webhook self-heals (see PrReconcileService, which
    // enqueues this job for every still-open PR on a schedule). MERGED is
    // terminal: never let an in-flight stale fetch revert a merged PR back to
    // OPEN/CLOSED — only forward transitions are applied.
    const existing = await this.prRepo.findOne({
      where: { repoFullName, prNumber },
      select: { state: true },
    });
    const applyState = !(existing?.state === "MERGED" && state !== "MERGED");

    if (applyState && existing && existing.state !== state) {
      this.logger.warn(
        `State drift corrected for ${repoFullName}#${prNumber}: ` +
          `${existing.state} → ${state} (missed webhook)`,
      );
    }

    // Cast past the entity's non-null column types: merged_at/closed_at/
    // merged_by_login are nullable in the DB, and writing null is correct
    // (clears them on a reopened PR).
    const update = {
      closingIssueNumbers: currentClosingIssueNumbers,
      body,
      lastEditedAt,
      ...(applyState ? { state, mergedAt, closedAt, mergedByLogin } : {}),
    } as QueryDeepPartialEntity<PullRequest>;

    await this.prRepo.update({ repoFullName, prNumber }, update);

    // Issue solver attribution is closure-driven (ISSUE_CLOSURE jobs read
    // ClosedEvent.closer). PR metadata only refreshes the PR-side text view
    // of which issues this PR claims to close — it never writes
    // issues.solved_by_pr.
  }

  private async handlePrFiles(data: PrFilesJobData): Promise<void> {
    const { repoFullName, prNumber } = data;
    this.logger.log(`Fetching PR files for ${repoFullName}#${prNumber}`);

    const generation = {
      headSha: data.expectedHeadSha ?? null,
      baseSha: data.expectedBaseSha ?? null,
    };

    await this.fetcher.fetchAndStorePrFiles(repoFullName, prNumber);

    const updateResult = await this.prRepo.update(
      this.prGenerationCriteria(repoFullName, prNumber, generation),
      { scoringDataStored: true },
    );

    if (!updateResult.affected) {
      await this.handleStalePrFilesJob(repoFullName, prNumber);
    }
  }

  private async handleBackfill(
    repoFullName: string,
    days: number,
  ): Promise<void> {
    this.logger.log(`Backfilling ${repoFullName} — last ${days} days`);

    const sinceDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

    // Fetch and upsert PRs. Each entry carries per-PR gating flags computed
    // against the pre-upsert stored row so the backfill (a safety net behind
    // real-time webhooks) only re-fetches what actually changed.
    const prs = await this.fetcher.backfillPullRequests(
      repoFullName,
      sinceDate,
    );
    this.logger.log(`Backfilled ${prs.length} PRs from ${repoFullName}`);

    // Fetch and upsert issues before PR metadata jobs can link solved_by_pr.
    const issueCount = await this.fetcher.backfillIssues(
      repoFullName,
      sinceDate,
    );
    this.logger.log(`Backfilled ${issueCount} issues from ${repoFullName}`);

    // Enqueue follow-up jobs, gated per-PR: PR_METADATA when GitHub's
    // updatedAt moved, PR_FILES when content is missing or head/base SHA moved.
    let metadataEnqueued = 0;
    let metadataSkipped = 0;
    let filesEnqueued = 0;
    let filesSkipped = 0;

    for (const {
      prNumber,
      headSha,
      baseSha,
      needsFilesJob,
      needsMetadataJob,
    } of prs) {
      if (needsMetadataJob) {
        await this.fetchQueue.add(
          FETCH_JOBS.PR_METADATA,
          { repoFullName, prNumber },
          {
            jobId: `meta-${repoFullName}-${prNumber}`,
            removeOnComplete: true,
            // Match the webhook handler — failed metadata jobs must not squat
            // on the stable per-PR jobId (#75).
            removeOnFail: true,
            attempts: 3,
            backoff: { type: "exponential", delay: 5000 },
          },
        );
        metadataEnqueued += 1;
      } else {
        metadataSkipped += 1;
      }

      if (needsFilesJob) {
        await this.enqueuePrFilesJob(
          repoFullName,
          prNumber,
          headSha ?? null,
          baseSha ?? null,
        );
        filesEnqueued += 1;
      } else {
        filesSkipped += 1;
      }
    }

    // Single greppable summary line for the mirror docker logs
    // (`docker logs ghm-das`) when debugging rate-limit / churn issues.
    this.logger.log(
      `[backfill-summary] repo=${repoFullName} window_days=${days} ` +
        `prs_in_window=${prs.length} ` +
        `meta_enqueued=${metadataEnqueued} meta_skipped=${metadataSkipped} ` +
        `files_enqueued=${filesEnqueued} files_skipped=${filesSkipped} ` +
        `issues_backfilled=${issueCount}`,
    );
  }

  private async handleStalePrFilesJob(
    repoFullName: string,
    prNumber: number,
  ): Promise<void> {
    await this.prRepo.update(
      { repoFullName, prNumber },
      { scoringDataStored: false },
    );

    const pr = await this.prRepo.findOneBy({ repoFullName, prNumber });
    if (!pr) return;

    await this.enqueuePrFilesJob(
      repoFullName,
      prNumber,
      pr.headSha ?? null,
      pr.baseSha ?? null,
    );
  }

  private async enqueuePrFilesJob(
    repoFullName: string,
    prNumber: number,
    expectedHeadSha: string | null,
    expectedBaseSha: string | null,
  ): Promise<void> {
    await this.fetchQueue.add(
      FETCH_JOBS.PR_FILES,
      { repoFullName, prNumber, expectedHeadSha, expectedBaseSha },
      {
        jobId: prFilesJobId(
          repoFullName,
          prNumber,
          expectedHeadSha,
          expectedBaseSha,
        ),
        removeOnComplete: true,
        removeOnFail: true,
        attempts: 3,
        backoff: { type: "exponential", delay: 5000 },
      },
    );
  }

  private prGenerationCriteria(
    repoFullName: string,
    prNumber: number,
    generation: PrFilesGeneration,
  ): Record<string, unknown> {
    return {
      repoFullName,
      prNumber,
      headSha: generation.headSha ?? IsNull(),
      baseSha: generation.baseSha ?? IsNull(),
    };
  }

  private uniqueIssueNumbers(issueNumbers: number[]): number[] {
    return [...new Set(issueNumbers)];
  }
}
