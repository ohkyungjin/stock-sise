# 한국 주식 시세 데이터 수집 시스템

## 프로젝트 개요

이 프로젝트는 한국 주식 시장(KOSPI, KOSDAQ)의 시세 데이터를 자동으로 수집하는 시스템입니다. 키움증권(Kiwoom Securities)과 한국투자증권(Korea Investment & Securities)의 API를 활용하여 종목 정보와 일별 시세 데이터를 수집하고, 분석 가능한 CSV 형식으로 저장합니다.

### 주요 기능

- **종목 정보 수집**: 키움증권 API를 통해 KOSPI, KOSDAQ 종목 리스트 조회
- **일별 시세 데이터 수집**: 한국투자증권 API를 통해 개별 종목의 일별 시세 데이터 조회
- **데이터 저장**: 수집된 데이터를 CSV 파일로 저장
- **배치 처리**: 멀티프로세싱을 통한 대량 데이터 수집 가속화
- **API 서버**: FastAPI를 활용한 RESTful API 서버 제공
- **자동화 지원**: n8n 워크플로우와의 연동을 통한 자동화 지원

## 시스템 구조

### 파일 구조

```
├── api/                   # API 클라이언트 모듈
│   ├── base_client.py     # 기본 API 클라이언트 클래스
│   ├── korea_investment_client.py  # 한국투자증권 API 클라이언트
│   ├── kiwoom_client.py   # 키움증권 API 클라이언트
│   └── token_manager.py   # API 토큰 관리 모듈
├── config/                # 설정 파일
│   ├── settings.py        # 기본 설정 값
│   └── tokens/            # 토큰 저장 디렉토리
├── utils/                 # 유틸리티 모듈
│   └── error_handler.py   # API 오류 처리 모듈
├── collectors/            # 데이터 수집 모듈
├── data/                  # 데이터 저장 디렉토리
├── logs/                  # 로그 파일 저장 디렉토리
├── n8n/                   # n8n 워크플로우 JSON 파일
├── main.py                # CLI 애플리케이션 진입점
├── main_api.py            # FastAPI 서버 진입점
├── requirements.txt       # 의존성 패키지 목록
└── .env                   # 환경 변수 파일
```

### 아키텍처

1. **토큰 관리 시스템**
   - `TokenManager` 클래스: API 토큰 발급, 저장, 갱신 관리
   - 토큰 유효성 검증 및 만료 시 자동 재발급
   - 발급된 토큰을 파일로 저장하여 재사용

2. **API 클라이언트**
   - `StockAPIClient` 기본 클래스
   - `KoreaInvestmentAPIClient`: 한국투자증권 API 연동
   - `KiwoomAPIClient`: 키움증권 API 연동

3. **데이터 수집 및 처리**
   - 멀티프로세싱을 활용한 병렬 데이터 수집
   - 예외 처리 및 로깅
   - CSV 포맷으로 데이터 저장

4. **API 서버**
   - FastAPI 기반 RESTful API 제공
   - 비동기 작업 처리 (배경 작업)
   - 웹훅을 통한 외부 시스템 연동

5. **자동화 워크플로우**
   - n8n 워크플로우를 통한 정기적인 데이터 수집 자동화
   - 텔레그램을 통한 작업 상태 알림

## 설치 방법

### 사전 요구사항

- Python 3.8 이상
- 키움증권 API 키와 시크릿 키
- 한국투자증권 API 키와 시크릿 키
- (선택사항) n8n 서버

### 설치 단계

1. 저장소 복제
   ```bash
   git clone <repository-url>
   cd auth4
   ```

2. 가상 환경 생성 및 활성화
   ```bash
   python -m venv venv
   # 윈도우
   venv\Scripts\activate
   # 리눅스/맥
   source venv/bin/activate
   ```

3. 의존성 패키지 설치
   ```bash
   pip install -r requirements.txt
   ```

4. 설정 파일 생성
   `.env` 파일을 프로젝트 루트 디렉토리에 생성하고 API 키 정보 입력:
   ```
   KIWOOM_APP_KEY=YOUR_KIWOOM_APP_KEY
   KIWOOM_APP_SECRET=YOUR_KIWOOM_APP_SECRET
   KIS_APP_KEY=YOUR_KIS_APP_KEY
   KIS_APP_SECRET=YOUR_KIS_APP_SECRET
   ```

5. 디렉토리 생성
   ```bash
   mkdir -p data logs config/tokens
   ```

## 사용 방법

### CLI 모드

명령행에서 직접 주식 시세 데이터를 수집하는 방법:

```bash
# 오늘 날짜의 모든 종목 시세 데이터 수집
python main.py

# 특정 날짜의 시세 데이터 수집
python main.py --date 20240301

# 날짜 범위로 시세 데이터 수집
python main.py --start 20240301 --end 20240331

# 특정 종목만 수집
python main.py --stock_code 005930 --name 삼성전자
```

### API 서버 모드

FastAPI 서버를 실행하여 API를 통해 데이터 수집:

```bash
# API 서버 실행
python main_api.py
```

서버 실행 후, 다음 API 엔드포인트를 사용할 수 있습니다:

- `GET /api/stock-prices/today`: 오늘 날짜 시세 데이터 수집
- `POST /api/stock-prices/date`: 특정 날짜 시세 데이터 수집
- `POST /api/stock-prices/range`: 날짜 범위로 시세 데이터 수집
- `GET /api/tasks/{task_id}`: 작업 상태 확인
- `GET /api/download/{task_id}`: 생성된 CSV 파일 다운로드

API 문서는 서버 실행 후 `http://localhost:8000/docs`에서 확인할 수 있습니다.

### n8n 워크플로우

n8n에서 워크플로우를 가져오는 방법:

1. n8n 대시보드에서 "Workflows" 메뉴로 이동
2. "Import from File" 버튼 클릭
3. `n8n/daily-stock-collection.json` 또는 `n8n/custom-stock-collection.json` 파일 선택
4. 필요한 경우 텔레그램 API 자격 증명 설정

## 워크플로우 예시

### 일일 데이터 수집 워크플로우

매일 오후 6시에 실행되어 당일 주식 시세 데이터를 수집하고 텔레그램으로 알림을 보냅니다.

### 사용자 요청 데이터 수집 워크플로우

사용자가 지정한 날짜 범위의 주식 시세 데이터를 수집하고 텔레그램으로 알림을 보냅니다.

## 주의사항

- API 호출 횟수에 제한이 있을 수 있으므로 과도한 요청은 피해주세요.
- 수집된 데이터는 투자 결정에 직접 사용하기 전에 검증해야 합니다.
- 증권사 API 사용 정책을 준수해야 합니다.

## 의존성 패키지

- requests: HTTP 요청 처리
- python-dotenv: 환경 변수 관리
- loguru: 로깅 관리
- apscheduler: 작업 스케줄링
- fastapi: API 서버
- uvicorn: ASGI 서버

## 라이선스

이 프로젝트는 개인 및 교육용으로 제공됩니다. 