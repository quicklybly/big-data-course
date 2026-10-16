FROM apache/superset:5.0.0

USER root
RUN uv pip install --no-cache --python /app/.venv/bin/python psycopg2-binary trino
USER superset
