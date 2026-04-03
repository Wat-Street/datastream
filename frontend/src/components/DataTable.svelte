<script>
  let { rows, onrowclick } = $props();

  // derive column headers from keys of the first data entry
  let columns = $derived(() => {
    if (!rows || rows.length === 0) return [];
    for (const row of rows) {
      if (row.data && row.data.length > 0) {
        return Object.keys(row.data[0]);
      }
    }
    return [];
  });
</script>

{#if !rows || rows.length === 0}
  <p class="empty">no data for this range</p>
{:else}
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>timestamp</th>
          {#each columns() as col (col)}
            <th>{col}</th>
          {/each}
        </tr>
      </thead>
      <tbody>
        {#each rows as row (row.timestamp)}
          {#each row.data as entry, i (i)}
            <tr
              onclick={() => onrowclick({ timestamp: row.timestamp, ...entry })}
            >
              {#if i === 0}
                <td class="ts" rowspan={row.data.length}>{row.timestamp}</td>
              {/if}
              {#each columns() as col (col)}
                <td>{entry[col] ?? ""}</td>
              {/each}
            </tr>
          {/each}
        {/each}
      </tbody>
    </table>
  </div>
{/if}

<style>
  .empty {
    color: var(--color-text-muted);
    padding: var(--space-lg) 0;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.875rem;
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
    white-space: nowrap;
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
    white-space: nowrap;
  }

  .ts {
    font-family: var(--font-mono);
    font-size: 0.8rem;
    color: var(--color-text-muted);
    vertical-align: top;
  }
</style>
