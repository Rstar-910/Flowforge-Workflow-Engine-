FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY migrations ./migrations
COPY alembic.ini ./
RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 flowforge

USER flowforge
CMD ["flowforge", "api", "--host", "0.0.0.0"]
