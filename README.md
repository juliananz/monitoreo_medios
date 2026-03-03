# Media Monitoring System – Mexico / Coahuila

Automated media monitoring pipeline focused on economic activity, investment,
employment, and risk/opportunity signals at national, international, and
municipal levels.

## Features
- RSS ingestion (national, international, Google News)
- Deduplication
- Thematic classification
- Risk vs opportunity detection
- Daily structured outputs

## Tech stack
- Python
- SQLite
- RSS
- Pandas
- Jupyter

## Running with Docker

```bash
# Build the image
docker compose build

# Start the dashboard (http://localhost:8501)
docker compose up dashboard

# Run the pipeline once (writes to the shared SQLite volume)
docker compose run --rm pipeline
```

The SQLite database is stored in a named Docker volume (`sqlite_data`) shared
between both services, so data produced by the pipeline is immediately visible
in the dashboard.

### Ollama (LLM summaries)

Ollama is **not containerized** and must run on the host machine:

```bash
ollama serve          # keep this running in a separate terminal
```

The pipeline's LLM summary step (`resumen_diario_llm.py`) connects to
`http://localhost:11434`. From inside a Docker container `localhost` refers to
the container itself, so the LLM step will be skipped gracefully if Ollama is
unreachable. To expose host Ollama to the containers on Linux add:

```yaml
# docker-compose.yml, under the pipeline service:
extra_hosts:
  - "host.docker.internal:host-gateway"
```

and set `OLLAMA_URL=http://host.docker.internal:11434/api/generate` as an
environment variable. On Docker Desktop (Windows / macOS) `host.docker.internal`
resolves automatically without the extra_hosts entry.

## Status
MVP – under active development
