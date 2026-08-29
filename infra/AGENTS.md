# Infrastructure (`infra`)

The service, Postgres database, and Caddy reverse proxy each run in their own Docker container.
Containers communicate over a Docker-managed internal network. Only Caddy exposes host ports
(80/443); internal services (builder, postgres, pgweb) have no host port mappings in production.
`builders/scripts/` is mounted as a volume into the builder container at runtime, so scripts can
be updated without rebuilding the image.

## Network topology

```
Internet --> [Caddy :80/:443] --internal--> [builder:3000]
                                            [postgres:5432]  (no host port)
                                            [pgweb:8080]     (no host port)
```

Caddy terminates TLS and reverse proxies to the builder over the Docker bridge network. Internal
traffic stays plain HTTP.

## Caddy reverse proxy

Caddy provides automatic HTTPS via Let's Encrypt with minimal config. The domain is set via the
`DOMAIN` env var in `infra/.env`.

- Production: `DOMAIN=datastream.yourdomain.com`, Caddy auto-provisions a Let's Encrypt cert.
- Local Docker: `DOMAIN=localhost` uses Caddy's internal CA (self-signed), or `DOMAIN=:80` for
  plain HTTP.
- Local dev (`just backend-dev`): unaffected, hits uvicorn on localhost:3000 directly.

## Dev overlay

`infra/docker-compose.dev.yml` re-exposes internal service ports for local development
(postgres:5432, builder:3000, pgweb:8080). Use `just docker-up-dev` to start with the overlay, or
`just docker-up` for production mode.

## Directory layout

```
infra/
  docker-compose.yml      production compose (only Caddy ports exposed)
  docker-compose.dev.yml  dev overlay (re-exposes internal ports)
  .env                    environment variables (DB credentials, DOMAIN, etc.)
  builder/
    Dockerfile
    entrypoint.sh
  caddy/
    Caddyfile
  postgres/
    Dockerfile
```
