FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# kis_config 폴더 생성 (토큰 파일 저장용)
RUN mkdir -p /app/kis_config

COPY . .

ENV PORT=8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8080/health', timeout=3)"

CMD ["python", "main.py"]
