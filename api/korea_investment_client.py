from loguru import logger
import time
from datetime import datetime, timedelta

from api.base_client import StockAPIClient
from config.settings import KIS_API_HOST


class KoreaInvestmentAPIClient(StockAPIClient):
    """한국투자증권 API 클라이언트"""
    
    def __init__(self, token_manager):
        super().__init__(token_manager)
        self.base_url = KIS_API_HOST
        # 종목명 저장용 딕셔너리
        self.stock_names = {}
    
    def get_headers(self, tr_id=None):
        """한국투자증권 API 요청 헤더 생성"""
        token_data = self.token_manager.get_token()
        headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'authorization': f'Bearer {token_data.get("access_token", "")}',
            'appkey': self.token_manager.app_key,
            'appsecret': self.token_manager.app_secret,
        }
        
        # TR ID가 지정된 경우 추가
        if tr_id:
            headers['tr_id'] = tr_id
        
        return headers
    
    def get_stock_list(self, market_type):
        """시장별 종목 리스트 조회 (한국투자증권은 미구현)"""
        logger.warning("한국투자증권 API에서는 종목 리스트 조회 기능을 제공하지 않습니다.")
        return {"output1": []}
    
    def get_daily_price(self, stock_code, period='D', is_adjusted=True, start_date=None, end_date=None, max_count=None):
        """일별 주가 데이터 조회
        stock_code: 종목코드 (예: '005930')
        period: 'D'(일), 'W'(주), 'M'(월), 'Y'(년)
        is_adjusted: 수정주가 반영 여부 (True/False)
        start_date: 조회 시작일(YYYYMMDD)
        end_date: 조회 종료일(YYYYMMDD)
        max_count: 최대 조회 데이터 수
        """
        # 입력값 검증
        if not stock_code or not isinstance(stock_code, str):
            logger.error(f"유효하지 않은 종목코드: {stock_code}")
            return {"output2": [], "success": False, "error": "유효하지 않은 종목코드"}
            
        # 시작일과 종료일 설정
        if not start_date or not end_date:
            today = datetime.now().strftime('%Y%m%d')
            # 기본값으로 최근 100일 데이터 조회
            if not end_date:
                end_date = today
            if not start_date:
                # 시작일 계산 (종료일로부터 100일 전)
                end_dt = datetime.strptime(end_date, '%Y%m%d')
                start_dt = end_dt - timedelta(days=100)
                start_date = start_dt.strftime('%Y%m%d')
        
        # API 엔드포인트
        endpoint = '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice'
        
        # 결과 저장용 리스트
        all_data = []
        
        # 연속 조회 관련 변수
        tr_cont = ""  # 초기 조회는 빈 문자열
        
        # API 연속 호출을 위한 루프
        while True:
            try:
                params = {
                    'FID_COND_MRKT_DIV_CODE': 'J',    # KRX
                    'FID_INPUT_ISCD': stock_code,      # 종목코드
                    'FID_INPUT_DATE_1': start_date,    # 조회 시작일
                    'FID_INPUT_DATE_2': end_date,      # 조회 종료일
                    'FID_PERIOD_DIV_CODE': period,     # 기간분류코드
                    'FID_ORG_ADJ_PRC': '1' if is_adjusted else '0'  # 수정주가(1)/원주가(0)
                }
                
                headers = self.get_headers('FHKST03010100')  # TR ID 설정
                
                # 연속 조회 설정
                if tr_cont:
                    headers.update({'tr_cont': tr_cont})
                
                response = self.execute_request('GET', endpoint, params=params, headers=headers)
                
                # 응답 확인
                if not response.get('success', False):
                    error_msg = response.get('error', '알 수 없는 오류')
                    logger.error(f"일별 주가 조회 실패: {error_msg}")
                    break
                
                # 응답 데이터 추출
                response_data = response.get('data', {})
                
                # 응답에서 데이터 추출 (output2가 차트 데이터)
                if 'output2' in response_data and isinstance(response_data['output2'], list):
                    data_chunk = response_data['output2']
                    all_data.extend(data_chunk)
                    
                    # 종목명 정보 저장
                    if 'output1' in response_data and 'hts_kor_isnm' in response_data['output1']:
                        stock_name = response_data['output1']['hts_kor_isnm']
                        self.stock_names[stock_code] = stock_name
                    
                    # 데이터가 없거나, 최대 개수에 도달한 경우 종료
                    if not data_chunk or (max_count and len(all_data) >= max_count):
                        break
                    
                    # 연속 조회 필요 여부 확인
                    resp_headers = response_data.get('_response_headers', {})
                    tr_cont_value = resp_headers.get('tr_cont', '')
                    
                    # 연속 조회가 필요 없는 경우 종료
                    if tr_cont_value != 'M':
                        break
                    
                    # 연속 조회 시 tr_cont 값 설정
                    tr_cont = 'N'
                    
                    # 연속 조회 시 잠시 대기 (API 제한 방지)
                    time.sleep(0.5)
                else:
                    # 응답 데이터가 없는 경우 종료
                    logger.warning(f"일별 주가 조회 응답에 output2 데이터가 없습니다.")
                    break
                
            except Exception as e:
                logger.error(f"일별 주가 조회 실패: {str(e)}")
                break
        
        # 최대 개수 제한
        if max_count and len(all_data) > max_count:
            all_data = all_data[:max_count]
        
        # 최종 응답 생성
        return {
            'output2': all_data,
            'rt_cd': '0',
            'msg_cd': 'MCA00000', 
            'msg1': '정상처리',
            'success': True if all_data else False
        }