# --- build the dashboard ---
FROM node:22-alpine AS web
WORKDIR /web
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

# --- API + pipeline ---
FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 STATIC_DIR=/srv/static
WORKDIR /srv
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app/ app/
COPY evals/ evals/
COPY --from=web /web/dist /srv/static
RUN useradd --create-home sentinel
USER sentinel
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/health')"
CMD ["uvicorn", "app.api.server:app", "--host", "0.0.0.0", "--port", "8000"]
