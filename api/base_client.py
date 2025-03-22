from abc import ABC, abstractmethod
from loguru import logger
import json
import requests
import sys
from api.token_manager import TokenExpiredException, TokenFailedException


class ApiResponse:
    """API 응답을 표준화하는 클래스"""
    
    def __init__(self, success=True, data=None, error=None, code=None, message=None):
        self.success = success
        self.data = data or {}
        self.error = error
        self.code = code
        self.message = message
    
    def to_dict(self):
        """응답을 딕셔너리로 변환"""
        return {
            'success': self.success,
            'data': self.data,
            'error': self.error,
            'code': self.code,
            'message': self.message
        }
    
    @classmethod
    def success_response(cls, data):
        """성공 응답 생성"""
        return cls(success=True, data=data)
    
    @classmethod
    def error_response(cls, error, code=None, message=None):
        """오류 응답 생성"""
        return cls(success=False, error=error, code=code, message=message)


class StockAPIClient(ABC):
    """주식 API 클라이언트 추상 기본 클래스"""
    
    def __init__(self, token_manager):
        self.token_manager = token_manager
    
    @abstractmethod
    def get_headers(self):
        """API 요청 헤더 생성 (API별로 오버라이드 필요)"""
        pass
    
    def execute_request(self, method, endpoint, params=None, data=None, headers=None):
        """API 요청 실행"""
        if headers is None:
            try:
                headers = self.get_headers()
            except TokenFailedException as e:
                # 토큰 발급 실패 시 프로그램 종료
                logger.critical(f"치명적 오류: 토큰 발급 실패 - {str(e)}")
                sys.exit(1)
        
        url = self.base_url + endpoint
        logger.debug(f"API 요청: {method} {url}")
        
        try:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers,
                timeout=30  # 타임아웃 설정
            )
            
            # 헤더 정보 저장 (연속 조회 등에 필요)
            response_headers = {}
            for key, value in response.headers.items():
                response_headers[key.lower()] = value
            
            # 응답 상태 코드 확인
            if response.status_code == 401:
                logger.warning("토큰이 만료되었습니다. 토큰을 갱신합니다.")
                try:
                    # 토큰 갱신 시도
                    self.token_manager.refresh_token()
                    # 새 헤더로 다시 요청
                    new_headers = self.get_headers()
                    return self.execute_request(method, endpoint, params, data, new_headers)
                except TokenFailedException as e:
                    # 토큰 갱신 실패 시 프로그램 종료
                    logger.critical(f"치명적 오류: 토큰 갱신 실패 - {str(e)}")
                    sys.exit(1)
            
            if response.status_code != 200:
                error_msg = f"API 오류: 상태 코드 {response.status_code}"
                logger.error(f"{error_msg} - {response.text[:200]}")  # 긴 응답은 앞부분만 로깅
                return self._standardize_response({"error": error_msg}, success=False, 
                                                code=str(response.status_code))
            
            try:
                json_response = response.json()
                json_response['_response_headers'] = response_headers
                
                # 디버그 로깅 - API 응답 기본 정보
                if 'return_code' in json_response:
                    logger.debug(f"API 응답: 코드 {json_response['return_code']} - {json_response.get('return_msg', '')}")
                elif 'rt_cd' in json_response:
                    logger.debug(f"API 응답: 코드 {json_response['rt_cd']} - {json_response.get('msg1', '')}")
                
                return self._standardize_response(json_response)
            except json.JSONDecodeError:
                error_msg = "응답이 유효한 JSON 형식이 아닙니다"
                logger.error(f"{error_msg}: {response.text[:200]}")  # 긴 응답은 앞부분만 로깅
                return self._standardize_response({"error": error_msg}, success=False)
                
        except requests.exceptions.Timeout:
            error_msg = f"API 요청 타임아웃: {url}"
            logger.error(error_msg)
            return self._standardize_response({"error": error_msg}, success=False, code="TIMEOUT")
        except requests.exceptions.ConnectionError:
            error_msg = f"API 서버 연결 실패: {url}"
            logger.error(error_msg)
            return self._standardize_response({"error": error_msg}, success=False, code="CONNECTION_ERROR")
        except Exception as e:
            error_msg = f"API 요청 중 오류 발생: {str(e)}"
            logger.error(error_msg)
            return self._standardize_response({"error": error_msg}, success=False)
    
    def _standardize_response(self, response_data, success=True, code=None):
        """다양한 API 응답 형식을 표준화"""
        # 각 API별 응답 형식에 따라 하위 클래스에서 재정의할 수 있지만,
        # 기본적인 성공/오류 구조는 여기서 처리
        
        # 에러 코드 확인
        if 'error' in response_data:
            success = False
            message = response_data.get('error')
            if not code:
                code = 'UNKNOWN_ERROR'
        # 키움증권 응답 형식 (return_code가 문자열 또는 숫자로 올 수 있음)
        elif 'return_code' in response_data:
            return_code = response_data['return_code']
            # 문자열 또는 숫자 0이 성공
            if str(return_code) != '0':
                success = False
                code = str(return_code)  # 코드는 문자열로 표준화
                message = response_data.get('return_msg', '알 수 없는 오류')
            else:
                message = response_data.get('return_msg', '정상처리')
        # 한국투자증권 응답 형식
        elif 'rt_cd' in response_data and response_data['rt_cd'] != '0':
            success = False
            code = response_data.get('rt_cd')
            message = response_data.get('msg1', '알 수 없는 오류')
        else:
            message = None
            
        return ApiResponse(
            success=success,
            data=response_data,
            error=None if success else message,
            code=code,
            message=message
        ).to_dict()
    
    @abstractmethod
    def get_stock_list(self, market_type):
        """시장별 종목 리스트 조회 메서드"""
        pass