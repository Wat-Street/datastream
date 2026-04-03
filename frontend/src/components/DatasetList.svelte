<script>
  import { fetchDatasets } from '../lib/api.js';

  let { onselect } = $props();

  let datasets = $state([]);
  let loading = $state(true);
  let error = $state(null);

  async function load() {
    loading = true;
    error = null;
    try {
      const res = await fetchDatasets();
      datasets = res.datasets;
    } catch (e) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    load();
  });
</script>

<section>
  {#if loading}
    <p class="status">loading datasets...</p>
  {:else if error}
    <div class="error">
      <p>{error}</p>
      <button onclick={load}>retry</button>
    </div>
  {:else if datasets.length === 0}
    <p class="status">no datasets found</p>
  {:else}
    <table>
      <thead>
        <tr>
          <th>name</th>
          <th>version</th>
          <th>data</th>
        </tr>
      </thead>
      <tbody>
        {#each datasets as dataset}
          <tr onclick={() => onselect(dataset)}>
            <td class="name">{dataset.name}</td>
            <td class="version">{dataset.version}</td>
            <td>
              <span class="dot" class:has-data={dataset.has_data}></span>
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</section>

<style>
  section {
    width: 100%;
  }

  .status {
    color: var(--color-text-muted);
    padding: var(--space-lg) 0;
  }

  .error {
    color: var(--color-error);
    display: flex;
    align-items: center;
    gap: var(--space-md);
    padding: var(--space-lg) 0;
  }

  .error button {
    background: var(--color-surface);
    color: var(--color-text);
    border: 1px solid var(--color-border);
    padding: var(--space-xs) var(--space-md);
    border-radius: var(--radius-sm);
    cursor: pointer;
    font-size: 0.85rem;
  }

  .error button:hover {
    background: var(--color-surface-hover);
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  thead th {
    text-align: left;
    color: var(--color-text-muted);
    font-size: 0.75rem;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    padding: var(--space-sm) var(--space-md);
    border-bottom: 1px solid var(--color-border);
  }

  tbody tr {
    cursor: pointer;
    transition: background 0.1s;
  }

  tbody tr:hover {
    background: var(--color-surface-hover);
  }

  tbody td {
    padding: var(--space-sm) var(--space-md);
    border-bottom: 1px solid var(--color-border);
  }

  .name {
    font-weight: 500;
  }

  .version {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    color: var(--color-text-muted);
  }

  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--color-border);
  }

  .dot.has-data {
    background: var(--color-success);
  }
</style>
