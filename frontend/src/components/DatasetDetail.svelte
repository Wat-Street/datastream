<script>
  import { fetchData } from '../lib/api.js';
  import { defaultDateRange } from '../lib/format.js';
  import DataTable from './DataTable.svelte';
  import JsonModal from './JsonModal.svelte';

  let { name, version, onback } = $props();

  const defaults = defaultDateRange();
  let startDate = $state(defaults.start);
  let endDate = $state(defaults.end);
  let result = $state(null);
  let loading = $state(false);
  let error = $state(null);
  let modalRow = $state(null);

  // newest data first
  let reversedRows = $derived(result?.rows ? [...result.rows].reverse() : []);

  async function load() {
    loading = true;
    error = null;
    try {
      result = await fetchData(name, version, startDate, endDate);
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

  <div class="controls">
    <label>
      start
      <input type="date" bind:value={startDate} />
    </label>
    <label>
      end
      <input type="date" bind:value={endDate} />
    </label>
    <button class="load-btn" onclick={load} disabled={loading}>
      {loading ? 'loading...' : 'load'}
    </button>
  </div>

  {#if error}
    <div class="error">
      <p>{error}</p>
      <button onclick={load}>retry</button>
    </div>
  {/if}

  {#if result}
    <p class="meta">
      showing {result.returned_timestamps} of {result.total_timestamps} timestamps
    </p>
    <DataTable rows={reversedRows} onrowclick={handleRowClick} />
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

  .controls {
    display: flex;
    align-items: flex-end;
    gap: var(--space-md);
    margin-bottom: var(--space-lg);
    flex-wrap: wrap;
  }

  label {
    display: flex;
    flex-direction: column;
    gap: var(--space-xs);
    font-size: 0.75rem;
    color: var(--color-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.05em;
  }

  input[type='date'] {
    background: var(--color-surface);
    color: var(--color-text);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-sm);
    padding: var(--space-xs) var(--space-sm);
    font-family: var(--font-mono);
    font-size: 0.85rem;
  }

  input[type='date']:focus {
    outline: 1px solid var(--color-accent);
    border-color: var(--color-accent);
  }

  .load-btn {
    background: var(--color-accent);
    color: #fff;
    border: none;
    border-radius: var(--radius-sm);
    padding: var(--space-xs) var(--space-lg);
    font-size: 0.85rem;
    cursor: pointer;
    white-space: nowrap;
  }

  .load-btn:hover:not(:disabled) {
    background: var(--color-accent-hover);
  }

  .load-btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
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
</style>
