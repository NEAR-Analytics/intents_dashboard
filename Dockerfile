FROM ghcr.io/astral-sh/uv:python3.12-bookworm

# Environment and runtime defaults
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PORT=8080 \
    STREAMLIT_SERVER_HEADLESS=true \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

WORKDIR /app

# Install dependencies first (better caching)
COPY pyproject.toml uv.lock ./

# Create a local project venv and install locked deps
ENV UV_PROJECT_ENVIRONMENT=/app/.venv
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Copy application code
COPY . .

# Ensure the project venv is on PATH
ENV PATH="/app/.venv/bin:${PATH}"

# Cloud Run expects the container to listen on $PORT
EXPOSE 8080

# Start Streamlit on the Cloud Run port
ENTRYPOINT ["sh", "-c", "exec streamlit run main.py --server.address=0.0.0.0 --server.port=${PORT:-8080}"]


