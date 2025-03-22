import os
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 프로젝트 기본 경로
BASE_DIR = Path(__file__).resolve().parent.parent

# API 키 및 시크릿
KIWOOM_APP_KEY = os.getenv('KIWOOM_APP_KEY')
KIWOOM_APP_SECRET = os.getenv('KIWOOM_APP_SECRET')
KIS_APP_KEY = os.getenv('KIS_APP_KEY')
KIS_APP_SECRET = os.getenv('KIS_APP_SECRET')

# API 엔드포인트
KIWOOM_API_HOST = 'https://api.kiwoom.com'
KIS_API_HOST = 'https://openapi.koreainvestment.com:9443'

# 토큰 설정
TOKEN_STORAGE_PATH = Path('config/tokens')
TOKEN_STORAGE_PATH.mkdir(parents=True, exist_ok=True)

# 데이터 저장 경로
DATA_PATH = Path(os.getenv('DATA_PATH', BASE_DIR / 'data'))

# 경로가 없으면 생성
DATA_PATH.mkdir(parents=True, exist_ok=True)