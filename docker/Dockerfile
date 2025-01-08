FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean

RUN pip install --upgrade pip \
    && pip install poetry

WORKDIR /app

COPY . /app

RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

EXPOSE 8000

CMD ["uvicorn", "bdi_api.app:app", "--host", "0.0.0.0", "--port", "8000"]