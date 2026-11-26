import type { MetarResponse } from "../lib/metar";

export async function fetchMetar(apiBaseUrl: string): Promise<MetarResponse> {
  const url = `${apiBaseUrl}/api/metar`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`GET ${url} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as MetarResponse;
}
