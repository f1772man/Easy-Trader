#!/bin/bash
# 1. 중지 & 삭제
docker stop easy-trader && docker rm easy-trader

# 2. 재빌드
docker build -t easy-trader:latest .

# 3. 다시 실행
docker run -d --name easy-trader --env-file .env \
  -v ~/easy-trader/secrets:/app/secrets:ro \
  -v ~/easy-trader/kis_config:/app/kis_config \
  -v ~/easy-trader/DATA:/app/DATA \
  -p 8080:8080 --restart unless-stopped \
  easy-trader:latest