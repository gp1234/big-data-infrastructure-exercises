FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl build-essential && \
    rm -rf /var/lib/apt/lists/*

ENV POETRY_VERSION=1.6.1
RUN pip install "poetry==$POETRY_VERSION"

WORKDIR /app

COPY . .

RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi

EXPOSE 8000

CMD ["poetry", "run", "uvicorn", "bdi_api.app:app", "--host", "0.0.0.0", "--port", "8000"]