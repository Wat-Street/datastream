<script>
  let status = $state('');

  async function ping() {
    status = '...';
    try {
      const res = await fetch('/ping');
      status = res.ok ? 'OK' : `Error (${res.status})`;
    } catch {
      status = 'Error (network)';
    }
  }
</script>

<h1>Datastream</h1>
<button onclick={ping}>Ping API</button>
<p>{status}</p>
