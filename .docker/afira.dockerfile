FROM python:3.12-slim

# Preamble
WORKDIR /app
RUN apt-get update && apt-get install -y python3-venv && apt-get clean
COPY requirements.txt .
RUN python3 -m venv .venv && ./.venv/bin/pip install --upgrade pip && ./.venv/bin/pip install -r requirements.txt
COPY . .

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python3", "main.py"]
