const STORAGE_KEY = "datastream.api_key";

export function getApiKey(): string | null {
  return localStorage.getItem(STORAGE_KEY);
}

export function storeApiKey(key: string): void {
  localStorage.setItem(STORAGE_KEY, key);
}

export function clearStoredApiKey(): void {
  localStorage.removeItem(STORAGE_KEY);
}

/** masked display form of a stored key, e.g. "dsk_****cd12" */
export function maskApiKey(key: string): string {
  return `${key.slice(0, 4)}****${key.slice(-4)}`;
}
