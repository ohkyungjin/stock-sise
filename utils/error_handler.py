import time
import sys
from loguru import logger
from api.token_manager import TokenFailedException


class APIErrorHandler:
    """API 요청 에러 처리 및 재시도 클래스"""
    
    def __init__(self, max_retries=3, retry_delay=5):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
    
    def handle_request(self, request_func, *args, **kwargs):
        """재시도 메커니즘을 통한 API 요청 처리"""
        retries = 0
        last_error = None
        
        while retries < self.max_retries:
            try:
                return request_func(*args, **kwargs)
            except TokenFailedException as e:
                # 토큰 발급 실패는 재시도하지 않고 즉시 종료
                logger.critical(f"토큰 발급 실패: {str(e)}")
                sys.exit(1)  # 프로그램 종료
            except Exception as e:
                last_error = e
                retries += 1
                
                if '토큰' in str(e).lower() or 'token' in str(e).lower():
                    logger.warning(f"토큰 관련 오류 발생: {str(e)}")
                    # 토큰 갱신은 TokenManager에서 자동 처리됨
                elif isinstance(e, ConnectionError):
                    logger.warning(f"연결 오류 발생 ({retries}/{self.max_retries}): {str(e)}")
                else:
                    logger.error(f"API 요청 오류 ({retries}/{self.max_retries}): {str(e)}")
                
                # 마지막 시도가 아니면 대기 후 재시도
                if retries < self.max_retries:
                    wait_time = self.retry_delay * retries
                    logger.info(f"{wait_time}초 후 재시도합니다...")
                    time.sleep(wait_time)
        
        logger.error(f"최대 재시도 횟수 초과: {self.max_retries}")
        raise MaxRetriesExceededException(f"최대 재시도 횟수({self.max_retries})를 초과했습니다: {str(last_error)}")


class TokenExpiredException(Exception):
    """토큰 만료 예외"""
    pass


class MaxRetriesExceededException(Exception):
    """최대 재시도 초과 예외"""
    pass