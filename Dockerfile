FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY keywordExtraction.py .

# Job de execução única: extrai keywords dos eventos e encerra.
CMD ["python", "keywordExtraction.py"]
