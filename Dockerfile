FROM python:3.9-slim

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 패키지 업데이트 및 필요한 의존성 설치
RUN apt-get update  && apt-get install -y

# Python 패키지 설치
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 전체 복사
COPY . .

# 컨테이너가 시작될 때 실행할 명령어 설정