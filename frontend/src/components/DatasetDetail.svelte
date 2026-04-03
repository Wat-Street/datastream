<script>
  import { fetchData } from '../lib/api.js';
  import { defaultDateRange } from '../lib/format.js';
  import DataTable from './DataTable.svelte';
  import JsonModal from './JsonModal.svelte';

  const PAGE_SIZE = 50;

  let { name, version, onback } = $props();

  let result = $state(null);
  let loading = $state(false);
  let error = $state(null);
  let modalRow = $state(null);
  let page = $state(0);

  // newest data first
  let allRows = $derived(result?.rows ? [...result.rows].reverse() : []);
  let totalPages = $derived(Math.max(1, Math.ceil(allRows.length / PAGE_SIZE)));
  let pageRows = $derived(allRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE));

  async function load() {
    loading = true;
    error = null;
    try {
      const { start, end } = defaultDateRange();
      result = await fetchData(name, version, start, end);
      page = 0;
    } catch (e) {
      error = e.message;
      result = null;
    } finally {
      loading = false;
    }
  }

  // auto-fetch on mount
  $effect(() => {
    load();
  });

  function handleRowClick(rowData) {
    modalRow = rowData;
  }
</script>

<div class="detail">
  <button class="back" onclick={onback}>&larr; back</button>

  <h2>{name} <span class="ver">{version}</span></h2>

  {#if loading}
    <p class="status">loading...</p>
  {:else if error}
    <div class="error">
      <p>{error}</p>
      <button onclick={load}>retry</button>
    </div>
  {:else if result}
    <p class="meta">
      {result.returned_timestamps} timestamps &middot; page {page + 1} of {totalPages}
    </p>

    <DataTable rows={pageRows} onrowclick={handleRowClick} />

    {#if totalPages > 1}
      <div class="pagination">
        <button onclick={() => (page = page - 1)} disabled={page === 0}>
          &larr; newer
        </button>
        <span class="page-info">page {page + 1} / {totalPages}</span>
        <button onclick={() => (page = page + 1)} disabled={page >= totalPages - 1}>
          older &rarr;
        </button>
      </div>
    {/if}
  {/if}

  {#if modalRow}
    <JsonModal data={modalRow} onclose={() => (modalRow = null)} />
  {/if}
</div>

<style>
  .detail {
    width: 100%;
  }

  .back {
    background: none;
    border: none;
    color: var(--color-accent);
    cursor: pointer;
    font-size: 0.875rem;
    padding: 0;
    margin-bottom: var(--space-md);
  }

  .back:hover {
    color: var(--color-accent-hover);
  }

  h2 {
    font-size: 1.25rem;
    font-weight: 600;
    margin-bottom: var(--space-lg);
  }

  .ver {
    font-family: var(--font-mono);
    font-size: 0.85rem;
    color: var(--color-text-muted);
    font-weight: 400;
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
    margin-bottom: var(--space-md);
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

  .meta {
    font-size: 0.8rem;
    color: var(--color-text-muted);
    margin-bottom: var(--space-md);
  }

  .pagination {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: var(--space-lg);
    padding: var(--space-lg) 0;
  }

  .pagination button {
    background: var(--color-surface);
    color: var(--color-text);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-sm);
    padding: var(--space-xs) var(--space-md);
    font-size: 0.85rem;
    cursor: pointer;
  }

  .pagination button:hover:not(:disabled) {
    background: var(--color-surface-hover);
  }

  .pagination button:disabled {
    opacity: 0.4;
    cursor: not-allowed;
  }

  .page-info {
    font-size: 0.8rem;
    color: var(--color-text-muted);
    font-family: var(--font-mono);
  }
</style>
