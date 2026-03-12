export type CreateJobPayload = {
  query: string;
  source_cookies_netscape?: string;
  force?: boolean;
  reset?: boolean;
  no_parallel?: boolean;
  yt_search_n?: number;
};

export function normalizeNetscapeCookies(raw: unknown): string;
export function buildCookieCandidates(raw: unknown): string[];
export function buildCreateJobPayload(
  query: unknown,
  sourceCookiesNetscape: unknown,
  options?: {
    force?: boolean;
    reset?: boolean;
    no_parallel?: boolean;
    noParallel?: boolean;
    yt_search_n?: number;
    ytSearchN?: number;
  }
): CreateJobPayload;
