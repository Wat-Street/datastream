const BASE = '/api/v1';

/**
 * fetch all datasets from the catalog.
 * @returns {Promise<{datasets: Array<{name: string, version: string, has_data: boolean}>}>}
 */
export async function fetchDatasets() {
  const res = await fetch(`${BASE}/datasets`);
  if (!res.ok) {
    throw new Error(`failed to fetch datasets: ${res.status}`);
  }
  return res.json();
}

/**
 * fetch data for a specific dataset in a time range.
 * uses build-data=false so the frontend never triggers builds.
 * accepts both 200 (complete) and 206 (partial) as valid responses.
 * @param {string} name
 * @param {string} version
 * @param {string} start - ISO date string (YYYY-MM-DD)
 * @param {string} end - ISO date string (YYYY-MM-DD)
 * @returns {Promise<{dataset_name: string, dataset_version: string, total_timestamps: number, returned_timestamps: number, rows: Array}>}
 */
export async function fetchData(name, version, start, end) {
  const params = new URLSearchParams({
    start,
    end,
    'build-data': 'false',
  });
  const res = await fetch(`${BASE}/data/${name}/${version}?${params}`);
  // 200 = complete, 206 = partial data
  if (res.status !== 200 && res.status !== 206) {
    throw new Error(`failed to fetch data: ${res.status}`);
  }
  return res.json();
}
