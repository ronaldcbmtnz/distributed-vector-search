FROM rayproject/ray:2.60.1-py312-cpu

WORKDIR /workspace

COPY pyproject.toml README.md ./
COPY src ./src
COPY deploy/ray/check_cluster.py ./deploy/ray/check_cluster.py

# FastAPI's OpenTelemetry API floor is newer than the SDK bundled in the Ray base image.
RUN python -m pip install --no-cache-dir -e ".[ray]" "opentelemetry-sdk>=1.44,<2" \
    && python -m pip check

USER ray
