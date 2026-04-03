<script>
  import { highlightJson } from '../lib/format.js';

  let { data, onclose } = $props();

  let html = $derived(highlightJson(data));

  function handleKeydown(e) {
    if (e.key === 'Escape') onclose();
  }

  function handleBackdrop(e) {
    // close only if clicking the backdrop itself
    if (e.target === e.currentTarget) onclose();
  }
</script>

<svelte:window onkeydown={handleKeydown} />

<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
<div class="backdrop" onclick={handleBackdrop}>
  <div class="modal">
    <button class="close" onclick={onclose}>&times;</button>
    <pre><code>{@html html}</code></pre>
  </div>
</div>

<style>
  .backdrop {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.6);
    display: flex;
    align-items: center;
    justify-content: center;
    z-index: 100;
    padding: var(--space-xl);
  }

  .modal {
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    padding: var(--space-xl);
    max-width: 600px;
    width: 100%;
    max-height: 80vh;
    overflow-y: auto;
    position: relative;
  }

  .close {
    position: absolute;
    top: var(--space-sm);
    right: var(--space-md);
    background: none;
    border: none;
    color: var(--color-text-muted);
    font-size: 1.25rem;
    cursor: pointer;
    padding: var(--space-xs);
    line-height: 1;
  }

  .close:hover {
    color: var(--color-text);
  }

  pre {
    margin: 0;
    font-family: var(--font-mono);
    font-size: 0.85rem;
    line-height: 1.6;
    white-space: pre-wrap;
    word-break: break-word;
  }

  /* json syntax highlighting */
  pre :global(.json-key) {
    color: var(--json-key);
  }

  pre :global(.json-string) {
    color: var(--json-string);
  }

  pre :global(.json-number) {
    color: var(--json-number);
  }

  pre :global(.json-boolean) {
    color: var(--json-boolean);
  }

  pre :global(.json-null) {
    color: var(--json-null);
  }
</style>
