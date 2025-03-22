import os
import json
import time
import requests
from datetime import datetime, timedelta
from loguru import logger
from abc import abstractmethod
from pathlib import Path


class TokenManager:
    """기본 토큰 관리자 클래스"""
    
    def __init__(self, app_key, app_secret):
        if not app_key or not app_secret:
            raise ValueError("API 키와 시크릿은 필수 값입니다")
            
        self.app_key = app_key
        self.app_secret = app_secret
        self.token = None
        self.expires_at = None
        self.token_type = None
        self.refresh_token = None
        self.max_retries = 3
        self.retry_delay = 1
    
    def get_token(self):
        """유효한 토큰 반환"""
        if self.token is None or self._is_token_expired():
            success = self.issue_token()
            
            # 토큰 발급 실패 시 최대 3번 재시도
            retries = 0
            while not success and retries < self.max_retries:
                retries += 1
                logger.warning(f"토큰 발급 재시도 {retries}/{self.max_retries}...")
                time.sleep(self.retry_delay * retries)  # 지수 백오프
                success = self.issue_token()
                
            if not success:
                # 토큰 발급 실패 시 예외 발생
                error_msg = f"{self.__class__.__name__} 토큰 발급 실패"
                logger.error(error_msg)
                raise TokenFailedException(error_msg)
                
        return self.token
    
    def _is_token_expired(self):
        """토큰 만료 여부 확인"""
        if self.expires_at is None:
            return True
        # 만료 10분 전부터는 만료된 것으로 간주
        return datetime.now() > (self.expires_at - timedelta(minutes=10))
    
    @abstractmethod
    def issue_token(self):
        """토큰 발급 (하위 클래스에서 구현)"""
        pass
    
    def _save_token_to_file(self, token_data, filename):
        """토큰 정보를 파일에 저장"""
        try:
            # 경로 확인 및 생성
            directory = os.path.dirname(filename)
            os.makedirs(directory, exist_ok=True)
            
            # 현재 시간 추가
            token_data["issued_at"] = int(datetime.now().timestamp())
            
            # 파일에 저장
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(token_data, f, ensure_ascii=False)
                
            logger.info(f"토큰 정보가 저장되었습니다: {filename}")
            return True
        except Exception as e:
            logger.error(f"토큰 정보 저장 실패: {str(e)}")
            return False
    
    def _load_token_from_file(self, filename):
        """파일에서 토큰 정보 로드"""
        try:
            # 파일 존재 확인
            if not os.path.exists(filename):
                logger.info(f"토큰 파일이 존재하지 않습니다: {filename}")
                return None
                
            # 파일에서 읽기
            with open(filename, 'r', encoding='utf-8') as f:
                token_data = json.load(f)
                
            # 유효성 검사
            if not isinstance(token_data, dict):
                logger.warning(f"토큰 파일 형식이 유효하지 않습니다: {filename}")
                return None
                
            # 만료 시간 확인
            expires_dt = token_data.get("expires_dt")
            if expires_dt:
                try:
                    # YYYYMMDDHHMMSS 형식 변환
                    if len(expires_dt) == 14:
                        exp_year = int(expires_dt[0:4])
                        exp_month = int(expires_dt[4:6])
                        exp_day = int(expires_dt[6:8])
                        exp_hour = int(expires_dt[8:10])
                        exp_min = int(expires_dt[10:12])
                        exp_sec = int(expires_dt[12:14])
                        
                        expires_at = datetime(exp_year, exp_month, exp_day, exp_hour, exp_min, exp_sec)
                        
                        # 토큰이 만료되었는지 확인
                        if datetime.now() > expires_at:
                            logger.warning(f"저장된 토큰이 만료되었습니다. 만료 시간: {expires_at}")
                            return None
                        
                        # 만료 시간 저장
                        self.expires_at = expires_at
                except Exception as e:
                    logger.error(f"만료 시간 파싱 오류: {str(e)}")
                    return None
            
            return token_data
                
        except Exception as e:
            logger.error(f"토큰 파일 로드 실패: {str(e)}")
            return None
    
    def refresh_token(self):
        """토큰 갱신"""
        logger.info(f"{self.__class__.__name__} 토큰 갱신 시작")
        success = self.issue_token()
        if not success:
            raise TokenFailedException(f"{self.__class__.__name__} 토큰 갱신 실패")
        return success
        
    def _handle_token_response(self, response, token_field='access_token'):
        """토큰 응답 처리 공통 로직"""
        if not response:
            logger.error("토큰 요청에 대한 응답이 없습니다.")
            return None
            
        try:
            # 응답 상태 코드 확인
            if response.status_code != 200:
                # 상세 로그는 디버그 레벨로 변경
                logger.debug(f"토큰 발급 실패: 상태 코드 {response.status_code} - {response.text[:100]}...")
                logger.error(f"토큰 발급 실패: 상태 코드 {response.status_code}")
                return None
                
            # JSON 파싱 시도
            try:
                result = response.json()
            except Exception as e:
                logger.error(f"토큰 응답이 유효한 JSON 형식이 아닙니다: {str(e)}")
                return None
            
            # 토큰 필드 확인
            if token_field not in result:
                logger.error(f"토큰 응답에 '{token_field}' 필드가 없습니다: {result}")
                return None
                
            # 만료 시간 설정 (토큰 발급 시간 + 유효기간 - 10분(여유 시간))
            expires_in = result.get("expires_in", 86400)  # 기본값 24시간
            self.expires_at = datetime.now() + timedelta(seconds=expires_in) - timedelta(minutes=10)
            
            logger.info(f"{self.__class__.__name__} 토큰 발급 성공, 만료 시간: {self.expires_at}")
            return result
            
        except Exception as e:
            logger.error(f"토큰 응답 처리 중 오류 발생: {str(e)}")
            return None


class KoreaInvestmentTokenManager(TokenManager):
    """한국투자증권 토큰 관리자"""
    
    def __init__(self, app_key, app_secret):
        super().__init__(app_key, app_secret)
        self.token_file = Path("config/tokens/kis_token.json")
        
        # 저장된 토큰을 먼저 로드
        loaded_token = self._load_token_from_file(self.token_file)
        if loaded_token:
            # 토큰 정보 설정
            self.token = {
                "access_token": loaded_token.get("access_token"),
                "token_type": loaded_token.get("token_type", "Bearer"),
                "expires_in": 86400  # 기본 1일
            }
            self.token_type = loaded_token.get("token_type", "Bearer")
        else:
            # 저장된 토큰이 없으면 새로 발급 시도
            logger.info("저장된 한국투자증권 토큰이 없거나 만료되었습니다. 새로 발급합니다.")
            # 초기화 시 토큰 발급 실패 시 예외를 발생시키도록 수정
            if not self.issue_token():
                raise TokenFailedException("한국투자증권 토큰 초기 발급 실패")
    
    def issue_token(self):
        """한국투자증권 토큰 발급"""
        try:
            url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
            headers = {"content-type": "application/json"}
            data = {
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret
            }
            
            logger.debug(f"한국투자증권 토큰 발급 요청: {url}")
            
            try:
                response = requests.post(url, headers=headers, json=data, timeout=10)
            except requests.RequestException as e:
                logger.error(f"한국투자증권 토큰 요청 실패: {str(e)}")
                return False
                
            result = self._handle_token_response(response, token_field='access_token')
            
            if result:
                # 토큰 정보 저장
                self.token = {
                    "access_token": result.get("access_token"),
                    "token_type": result.get("token_type"),
                    "expires_in": result.get("expires_in")
                }
                
                # 개별 필드도 저장
                self.token_type = result.get("token_type")
                
                # 만료 시간 포맷팅 (YYYYMMDDHHMMSS)
                expires_dt = self.expires_at.strftime("%Y%m%d%H%M%S")
                
                # 파일에 저장할 데이터
                save_data = {
                    "access_token": result.get("access_token"),
                    "token_type": result.get("token_type", "Bearer"),
                    "expires_dt": expires_dt,
                    "rt_cd": "0",
                    "msg1": "정상처리"
                }
                
                # 파일에 저장
                self._save_token_to_file(save_data, self.token_file)
                
                return True
            return False
            
        except Exception as e:
            logger.error(f"한국투자증권 토큰 발급 중 오류 발생: {str(e)}")
            return False


class KiwoomTokenManager(TokenManager):
    """키움증권 토큰 관리자"""
    
    def __init__(self, app_key, app_secret):
        super().__init__(app_key, app_secret)
        self.token_file = Path("config/tokens/kiwoom_token.json")
        
        # 저장된 토큰을 먼저 로드
        loaded_token = self._load_token_from_file(self.token_file)
        if loaded_token:
            logger.info(f"저장된 키움증권 토큰을 사용합니다. 만료 시간: {self.expires_at}")
            # 토큰 정보 설정
            self.token = {
                "token": loaded_token.get("token"),
                "token_type": loaded_token.get("token_type", "bearer")
            }
            self.token_type = loaded_token.get("token_type", "bearer")
        else:
            # 저장된 토큰이 없으면 새로 발급 시도
            logger.info("저장된 키움증권 토큰이 없거나 만료되었습니다. 새로 발급합니다.")
            # 초기화 시 토큰 발급 실패 시 예외를 발생시키도록 수정
            if not self.issue_token():
                raise TokenFailedException("키움증권 토큰 초기 발급 실패")
    
    def issue_token(self):
        """키움증권 토큰 발급"""
        try:
            url = "https://api.kiwoom.com/oauth2/token"
            headers = {"Content-Type": "application/json;charset=UTF-8"}
            data = {
                "appkey": self.app_key,
                "appsecret": self.app_secret
            }
            
            logger.debug(f"키움증권 토큰 발급 요청: {url}")
            
            try:
                response = requests.post(url, headers=headers, json=data, timeout=10)
            except requests.RequestException as e:
                logger.error(f"키움증권 토큰 요청 실패: {str(e)}")
                return False
            
            try:
                result = response.json()
            except Exception as e:
                logger.error(f"키움증권 토큰 응답이 유효한 JSON 형식이 아닙니다: {str(e)}")
                return False
            
            # 응답 검사
            if response.status_code != 200:
                logger.error(f"키움증권 토큰 발급 실패: 상태 코드 {response.status_code}")
                return False
            
            # 토큰 필드 확인
            if "token" not in result:
                logger.error(f"키움증권 토큰 응답에 'token' 필드가 없습니다: {result}")
                return False
            
            # 반환 코드 검사
            return_code = result.get("return_code")
            if return_code != 0:
                return_msg = result.get("return_msg", "알 수 없는 오류")
                logger.error(f"키움증권 토큰 발급 실패: 코드 {return_code} - {return_msg}")
                return False
            
            # 만료 시간 설정
            expires_dt = result.get("expires_dt")
            if expires_dt and len(expires_dt) == 14:
                try:
                    exp_year = int(expires_dt[0:4])
                    exp_month = int(expires_dt[4:6])
                    exp_day = int(expires_dt[6:8])
                    exp_hour = int(expires_dt[8:10])
                    exp_min = int(expires_dt[10:12])
                    exp_sec = int(expires_dt[12:14])
                    
                    self.expires_at = datetime(exp_year, exp_month, exp_day, exp_hour, exp_min, exp_sec)
                    logger.info(f"키움증권 토큰 만료 시간: {self.expires_at}")
                except Exception as e:
                    logger.error(f"키움증권 만료 시간 파싱 오류: {str(e)}")
                    # 기본 만료 시간 설정 (24시간)
                    self.expires_at = datetime.now() + timedelta(hours=24) - timedelta(minutes=10)
            else:
                # 기본 만료 시간 설정 (24시간)
                self.expires_at = datetime.now() + timedelta(hours=24) - timedelta(minutes=10)
            
            # 토큰 정보 저장
            self.token = {
                "token": result.get("token"),
                "token_type": result.get("token_type", "bearer")
            }
            self.token_type = result.get("token_type", "bearer")
            
            # 파일에 저장할 데이터
            self._save_token_to_file(result, self.token_file)
            
            logger.info("키움증권 토큰 발급 성공")
            return True
            
        except Exception as e:
            logger.error(f"키움증권 토큰 발급 중 오류 발생: {str(e)}")
            return False
    
    def get_token_for_header(self):
        """API 헤더용 토큰 반환"""
        token_data = self.get_token()
        if isinstance(token_data, dict):
            return token_data.get("token", "")
        return ""


class TokenExpiredException(Exception):
    """토큰 만료 예외"""
    def __init__(self, message="토큰이 만료되었습니다"):
        self.message = message
        super().__init__(self.message)


class TokenFailedException(Exception):
    """토큰 발급 실패 예외"""
    def __init__(self, message="토큰 발급에 실패했습니다"):
        self.message = message
        super().__init__(self.message)