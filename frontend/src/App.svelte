<script>
  import DatasetList from './components/DatasetList.svelte';

  // view state: 'list' or 'detail'
  let view = $state('list');
  let selectedDataset = $state(null);

  function handleSelect(dataset) {
    selectedDataset = dataset;
    view = 'detail';
  }

  function handleBack() {
    selectedDataset = null;
    view = 'list';
  }
</script>

<header>
  <h1>Datastream</h1>
</header>

<main>
  {#if view === 'list'}
    <DatasetList onselect={handleSelect} />
  {:else if view === 'detail'}
    <p class="placeholder">dataset detail goes here</p>
  {/if}
</main>

<style>
  header {
    margin-bottom: var(--space-xl);
  }

  h1 {
    font-size: 1.5rem;
    font-weight: 600;
    letter-spacing: -0.02em;
  }

  .placeholder {
    color: var(--color-text-muted);
  }
</style>
