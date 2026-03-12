export function buildJobMediaRevisionToken(
  job?: {
    id?: string | null;
    render_finished_at?: number | null;
    finished_at?: number | null;
    last_updated_at?: number | null;
    started_at?: number | null;
    created_at?: number | null;
  } | null,
  channel?: string | null
): string | null;

export function appendMediaRevisionToken(url?: string | null, token?: string | null): string | null;
