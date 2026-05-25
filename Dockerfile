FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY k8s_inspector /app/k8s_inspector
COPY pyproject.toml /app/pyproject.toml
COPY README.md /app/README.md

ENTRYPOINT ["python", "-m", "k8s_inspector.cli"]
