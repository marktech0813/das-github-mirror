/* eslint-disable @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-assignment */
import { Injectable } from "@nestjs/common";
import { DataSource } from "typeorm";
import { buildPaginatedResponse, PaginationParams } from "./pagination";

const DEFAULT_SINCE_DAYS = 35;

// Column list (everything between SELECT and FROM) for the PR query. Shared by
// the scalar-`since` GET path and the per-repo `since` POST path so the two
// stay identical.
const PR_SELECT_COLUMNS = `
        LOWER(p.repo_full_name)         AS repo_full_name,
        p.pr_number,
        COALESCE(p.title, '')           AS title,
        p.body,
        p.state,
        p.author_github_id,
        COALESCE(p.author_login, '')    AS author_login,
        COALESCE(m_author.association, p.author_association) AS author_association,
        p.created_at,
        p.closed_at,
        p.merged_at,
        p.last_edited_at,
        p.merged_by_login,
        p.base_ref,
        p.head_ref,
        LOWER(p.head_repo_full_name)    AS head_repo_full_name,
        r.default_branch,
        p.head_sha,
        p.base_sha,
        p.merge_base_sha,
        COALESCE(p.additions, 0)        AS additions,
        COALESCE(p.deletions, 0)        AS deletions,
        COALESCE(p.commits_count, 0)    AS commits_count,
        p.scoring_data_stored,
        (p.last_edited_at IS NOT NULL AND p.merged_at IS NOT NULL AND p.last_edited_at > p.merged_at)
          AS edited_after_merge,
        CASE
          WHEN p.merged_at IS NOT NULL
            THEN ROUND(
              (EXTRACT(EPOCH FROM (NOW() - p.merged_at)) / 3600.0)::numeric, 2
            )::float8
          ELSE NULL
        END AS hours_since_merge,
        json_build_object(
          'maintainer_changes_requested_count', COALESCE(rs.maintainer_changes_requested_count, 0),
          'changes_requested_count',             COALESCE(rs.changes_requested_count, 0),
          'approved_count',                      COALESCE(rs.approved_count, 0),
          'commented_count',                     COALESCE(rs.commented_count, 0)
        ) AS review_summary,
        COALESCE((
          SELECT json_agg(json_build_object(
            'name',              plt.label_name,
            'actor_github_id',   plt.actor_github_id,
            'actor_association', plt.actor_association
          ))
          FROM pr_labels_by_actor plt
          WHERE plt.repo_full_name = p.repo_full_name
            AND plt.pr_number      = p.pr_number
        ), '[]'::json) AS labels,
        COALESCE((
          SELECT json_agg(json_build_object(
            'number',             li.issue_number,
            'title',              COALESCE(li.issue_title, ''),
            'state',              li.issue_state,
            'state_reason',       li.issue_state_reason,
            'author_github_id',   li.issue_author_github_id,
            'author_association', li.issue_author_association,
            'created_at',         li.issue_created_at,
            'closed_at',          li.issue_closed_at,
            'updated_at',         li.issue_updated_at,
            'is_transferred',     li.is_transferred,
            'solved_by_pr',       li.issue_solved_by_pr,
            'labels',             COALESCE((
              SELECT json_agg(json_build_object(
                'name',              ilt.label_name,
                'actor_github_id',   ilt.actor_github_id,
                'actor_association', ilt.actor_association
              ))
              FROM issue_labels_by_actor ilt
              WHERE ilt.repo_full_name = li.repo_full_name
                AND ilt.issue_number   = li.issue_number
            ), '[]'::json)
          ))
          FROM pr_linked_issues li
          WHERE li.repo_full_name = p.repo_full_name
            AND li.pr_number      = p.pr_number
        ), '[]'::json) AS linked_issues`;

// Column list for the issue query. Shared by the GET and POST paths.
const ISSUE_SELECT_COLUMNS = `
        LOWER(i.repo_full_name)         AS repo_full_name,
        i.issue_number,
        COALESCE(i.title, '')           AS title,
        i.state,
        i.state_reason,
        i.author_github_id,
        i.author_login,
        COALESCE(m_author.association, i.author_association) AS author_association,
        i.created_at,
        i.closed_at,
        i.updated_at,
        i.last_edited_at,
        i.is_transferred,
        i.solved_by_pr,
        COALESCE((
          SELECT json_agg(json_build_object(
            'name',              ilt.label_name,
            'actor_github_id',   ilt.actor_github_id,
            'actor_association', ilt.actor_association
          ))
          FROM issue_labels_by_actor ilt
          WHERE ilt.repo_full_name = i.repo_full_name
            AND ilt.issue_number   = i.issue_number
        ), '[]'::json) AS labels,
        (
          SELECT json_build_object(
            'pr_number',         sp.pr_number,
            'author_github_id',  sp.author_github_id,
            'state',             sp.state,
            'merged_at',         sp.merged_at,
            'hours_since_merge',
              CASE WHEN sp.merged_at IS NOT NULL
                THEN ROUND(
                  (EXTRACT(EPOCH FROM (NOW() - sp.merged_at)) / 3600.0)::numeric, 2
                )::float8
                ELSE NULL END,
            'edited_after_merge',
              (sp.last_edited_at IS NOT NULL
                AND sp.merged_at IS NOT NULL
                AND sp.last_edited_at > sp.merged_at),
            'head_sha',          sp.head_sha,
            'base_sha',          sp.base_sha,
            'merge_base_sha',    sp.merge_base_sha,
            'base_ref',          sp.base_ref,
            'head_ref',          sp.head_ref,
            'head_repo_full_name', LOWER(sp.head_repo_full_name),
            'default_branch',    sr.default_branch,
            'labels', COALESCE((
              SELECT json_agg(json_build_object(
                'name',              plt.label_name,
                'actor_github_id',   plt.actor_github_id,
                'actor_association', plt.actor_association
              ))
              FROM pr_labels_by_actor plt
              WHERE plt.repo_full_name = sp.repo_full_name
                AND plt.pr_number      = sp.pr_number
            ), '[]'::json),
            'review_summary', json_build_object(
              'maintainer_changes_requested_count', COALESCE(rs.maintainer_changes_requested_count, 0),
              'changes_requested_count',             COALESCE(rs.changes_requested_count, 0),
              'approved_count',                      COALESCE(rs.approved_count, 0),
              'commented_count',                     COALESCE(rs.commented_count, 0)
            )
          )
          FROM pull_requests sp
          LEFT JOIN pr_review_summary rs
            ON rs.repo_full_name = sp.repo_full_name
           AND rs.pr_number      = sp.pr_number
          LEFT JOIN repos sr
            ON sr.repo_full_name = sp.repo_full_name
          WHERE sp.repo_full_name = i.repo_full_name
            AND sp.pr_number      = i.solved_by_pr
            -- Skip null-author solving PRs (no one to credit)
            AND sp.author_github_id IS NOT NULL
            AND BTRIM(sp.author_github_id) <> ''
            -- Skip corrupted MERGED-without-merged_at shape
            AND NOT (sp.state = 'MERGED' AND sp.merged_at IS NULL)
        ) AS solving_pr`;

@Injectable()
export class MinersService {
  constructor(private readonly dataSource: DataSource) {}

  async getPullRequests(
    githubId: string,
    since: string,
    pagination: PaginationParams | null,
  ): Promise<{
    github_id: string;
    since: string;
    generated_at: string;
    pull_requests: unknown[];
    next_cursor?: string | null;
  }> {
    const cursor = pagination?.cursor ?? null;
    const keysetClause = cursor
      ? `AND (p.created_at, LOWER(p.repo_full_name), p.pr_number) < ($3::timestamptz, $4, $5::int)`
      : "";
    const limitClause = pagination ? `LIMIT $${cursor ? 6 : 3}` : "";
    const params: unknown[] = !pagination
      ? [githubId, since]
      : cursor
        ? [
            githubId,
            since,
            cursor.createdAt,
            cursor.repoFullName,
            cursor.number,
            pagination.limit + 1,
          ]
        : [githubId, since, pagination.limit + 1];

    const rows = await this.dataSource.query(
      `
      SELECT${PR_SELECT_COLUMNS}
      FROM pull_requests p
      LEFT JOIN pr_review_summary rs
        ON LOWER(rs.repo_full_name) = LOWER(p.repo_full_name)
       AND rs.pr_number      = p.pr_number
      LEFT JOIN repos r
        ON LOWER(r.repo_full_name) = LOWER(p.repo_full_name)
      LEFT JOIN maintainers m_author
        ON m_author.github_id = p.author_github_id
       AND m_author.repo_full_name = LOWER(p.repo_full_name)
      WHERE p.author_github_id = $1
        AND (
          (p.state = 'OPEN'   AND p.created_at >= $2)
          OR (p.state = 'MERGED' AND p.merged_at >= $2)
          OR (p.state = 'CLOSED' AND p.created_at >= $2)
        )
        ${keysetClause}
      ORDER BY p.created_at DESC, LOWER(p.repo_full_name) DESC, p.pr_number DESC
      ${limitClause}
      `,
      params,
    );

    if (!pagination) {
      return {
        github_id: githubId,
        since,
        generated_at: new Date().toISOString(),
        pull_requests: rows,
      };
    }

    const page = buildPaginatedResponse(
      rows as Record<string, unknown>[],
      pagination.limit,
      (row) => ({
        created_at: row.created_at as string,
        repo_full_name: row.repo_full_name as string,
        pr_number: row.pr_number as number,
      }),
    );

    return {
      github_id: githubId,
      since,
      generated_at: new Date().toISOString(),
      pull_requests: page.items,
      next_cursor: page.nextCursor,
    };
  }

  /**
   * Per-repo variant of getPullRequests: each repo is windowed by its own
   * `since`. `repoNames` and `sinceValues` are parallel arrays (same length and
   * order); repo names are already lowercased and timestamps already ISO. The
   * INNER JOIN to the unnested windows restricts results to the named repos.
   */
  async getPullRequestsByRepo(
    githubId: string,
    repoNames: string[],
    sinceValues: string[],
    pagination: PaginationParams | null,
  ): Promise<{
    github_id: string;
    since: null;
    generated_at: string;
    pull_requests: unknown[];
    next_cursor?: string | null;
  }> {
    const cursor = pagination?.cursor ?? null;
    const keysetClause = cursor
      ? `AND (p.created_at, LOWER(p.repo_full_name), p.pr_number) < ($4::timestamptz, $5, $6::int)`
      : "";
    const limitClause = pagination ? `LIMIT $${cursor ? 7 : 4}` : "";
    const params: unknown[] = !pagination
      ? [githubId, repoNames, sinceValues]
      : cursor
        ? [
            githubId,
            repoNames,
            sinceValues,
            cursor.createdAt,
            cursor.repoFullName,
            cursor.number,
            pagination.limit + 1,
          ]
        : [githubId, repoNames, sinceValues, pagination.limit + 1];

    const rows = await this.dataSource.query(
      `
      WITH windows AS (
        SELECT * FROM unnest($2::text[], $3::timestamptz[]) AS t(repo_full_name, since)
      )
      SELECT${PR_SELECT_COLUMNS}
      FROM pull_requests p
      JOIN windows w
        ON w.repo_full_name = LOWER(p.repo_full_name)
      LEFT JOIN pr_review_summary rs
        ON LOWER(rs.repo_full_name) = LOWER(p.repo_full_name)
       AND rs.pr_number      = p.pr_number
      LEFT JOIN repos r
        ON LOWER(r.repo_full_name) = LOWER(p.repo_full_name)
      LEFT JOIN maintainers m_author
        ON m_author.github_id = p.author_github_id
       AND m_author.repo_full_name = LOWER(p.repo_full_name)
      WHERE p.author_github_id = $1
        AND (
          (p.state = 'OPEN'   AND p.created_at >= w.since)
          OR (p.state = 'MERGED' AND p.merged_at >= w.since)
          OR (p.state = 'CLOSED' AND p.created_at >= w.since)
        )
        ${keysetClause}
      ORDER BY p.created_at DESC, LOWER(p.repo_full_name) DESC, p.pr_number DESC
      ${limitClause}
      `,
      params,
    );

    if (!pagination) {
      return {
        github_id: githubId,
        since: null,
        generated_at: new Date().toISOString(),
        pull_requests: rows,
      };
    }

    const page = buildPaginatedResponse(
      rows as Record<string, unknown>[],
      pagination.limit,
      (row) => ({
        created_at: row.created_at as string,
        repo_full_name: row.repo_full_name as string,
        pr_number: row.pr_number as number,
      }),
    );

    return {
      github_id: githubId,
      since: null,
      generated_at: new Date().toISOString(),
      pull_requests: page.items,
      next_cursor: page.nextCursor,
    };
  }

  async getIssues(
    githubId: string,
    since: string | null,
    pagination: PaginationParams | null,
  ): Promise<{
    github_id: string;
    since: string | null;
    generated_at: string;
    issues: unknown[];
    next_cursor?: string | null;
  }> {
    const cursor = pagination?.cursor ?? null;
    const keysetClause = cursor
      ? `AND (i.created_at, LOWER(i.repo_full_name), i.issue_number) < ($3::timestamptz, $4, $5::int)`
      : "";
    const limitClause = pagination ? `LIMIT $${cursor ? 6 : 3}` : "";
    const params: unknown[] = !pagination
      ? [githubId, since]
      : cursor
        ? [
            githubId,
            since,
            cursor.createdAt,
            cursor.repoFullName,
            cursor.number,
            pagination.limit + 1,
          ]
        : [githubId, since, pagination.limit + 1];

    const rows = await this.dataSource.query(
      `
      SELECT${ISSUE_SELECT_COLUMNS}
      FROM issues i
      LEFT JOIN maintainers m_author
        ON m_author.github_id = i.author_github_id
       AND m_author.repo_full_name = LOWER(i.repo_full_name)
      WHERE i.author_github_id = $1
        AND (
          (i.state = 'OPEN' AND ($2::timestamptz IS NULL OR i.created_at >= $2))
          OR (i.state = 'CLOSED' AND i.closed_at >= $2)
        )
        ${keysetClause}
      ORDER BY i.created_at DESC, LOWER(i.repo_full_name) DESC, i.issue_number DESC
      ${limitClause}
      `,
      params,
    );

    if (!pagination) {
      return {
        github_id: githubId,
        since,
        generated_at: new Date().toISOString(),
        issues: rows,
      };
    }

    const page = buildPaginatedResponse(
      rows as Record<string, unknown>[],
      pagination.limit,
      (row) => ({
        created_at: row.created_at as string,
        repo_full_name: row.repo_full_name as string,
        issue_number: row.issue_number as number,
      }),
    );

    return {
      github_id: githubId,
      since,
      generated_at: new Date().toISOString(),
      issues: page.items,
      next_cursor: page.nextCursor,
    };
  }

  /**
   * Per-repo variant of getIssues: each repo is windowed by its own `since`.
   * `repoNames` / `sinceValues` are parallel arrays as in getPullRequestsByRepo.
   * Every window `since` is a concrete timestamp (the controller rejects nulls),
   * so the OPEN branch has no NULL fallback.
   */
  async getIssuesByRepo(
    githubId: string,
    repoNames: string[],
    sinceValues: string[],
    pagination: PaginationParams | null,
  ): Promise<{
    github_id: string;
    since: null;
    generated_at: string;
    issues: unknown[];
    next_cursor?: string | null;
  }> {
    const cursor = pagination?.cursor ?? null;
    const keysetClause = cursor
      ? `AND (i.created_at, LOWER(i.repo_full_name), i.issue_number) < ($4::timestamptz, $5, $6::int)`
      : "";
    const limitClause = pagination ? `LIMIT $${cursor ? 7 : 4}` : "";
    const params: unknown[] = !pagination
      ? [githubId, repoNames, sinceValues]
      : cursor
        ? [
            githubId,
            repoNames,
            sinceValues,
            cursor.createdAt,
            cursor.repoFullName,
            cursor.number,
            pagination.limit + 1,
          ]
        : [githubId, repoNames, sinceValues, pagination.limit + 1];

    const rows = await this.dataSource.query(
      `
      WITH windows AS (
        SELECT * FROM unnest($2::text[], $3::timestamptz[]) AS t(repo_full_name, since)
      )
      SELECT${ISSUE_SELECT_COLUMNS}
      FROM issues i
      JOIN windows w
        ON w.repo_full_name = LOWER(i.repo_full_name)
      LEFT JOIN maintainers m_author
        ON m_author.github_id = i.author_github_id
       AND m_author.repo_full_name = LOWER(i.repo_full_name)
      WHERE i.author_github_id = $1
        AND (
          (i.state = 'OPEN' AND i.created_at >= w.since)
          OR (i.state = 'CLOSED' AND i.closed_at >= w.since)
        )
        ${keysetClause}
      ORDER BY i.created_at DESC, LOWER(i.repo_full_name) DESC, i.issue_number DESC
      ${limitClause}
      `,
      params,
    );

    if (!pagination) {
      return {
        github_id: githubId,
        since: null,
        generated_at: new Date().toISOString(),
        issues: rows,
      };
    }

    const page = buildPaginatedResponse(
      rows as Record<string, unknown>[],
      pagination.limit,
      (row) => ({
        created_at: row.created_at as string,
        repo_full_name: row.repo_full_name as string,
        issue_number: row.issue_number as number,
      }),
    );

    return {
      github_id: githubId,
      since: null,
      generated_at: new Date().toISOString(),
      issues: page.items,
      next_cursor: page.nextCursor,
    };
  }

  /**
   * Parse a `since` query param into an ISO timestamp. If not provided, defaults
   * to DEFAULT_SINCE_DAYS days ago (midnight UTC of that day).
   */
  static resolveSince(since?: string): string {
    if (since) return since;
    const d = new Date();
    d.setDate(d.getDate() - DEFAULT_SINCE_DAYS);
    d.setUTCHours(0, 0, 0, 0);
    return d.toISOString();
  }
}
