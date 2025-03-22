from loguru import logger

from api.base_client import StockAPIClient
from config.settings import KIWOOM_API_HOST


class KiwoomAPIClient(StockAPIClient):
    """키움증권 API 클라이언트"""
    
    def __init__(self, token_manager):
        super().__init__(token_manager)
        self.base_url = KIWOOM_API_HOST
    
    def get_headers(self, api_id=None, cont_yn='N', next_key=''):
        """키움증권 API 요청 헤더 생성"""
        token = self.token_manager.get_token_for_header()
        headers = {
            'Content-Type': 'application/json;charset=UTF-8',
            'authorization': f'Bearer {token}'
        }
        
        # API ID가 제공된 경우 추가 헤더 설정
        if api_id:
            headers.update({
                'cont-yn': cont_yn,
                'next-key': next_key,
                'api-id': api_id
            })
            
        return headers
    
    def get_stock_list(self, market_type):
        """시장별 종목 리스트 조회
        market_type: 0(코스피), 10(코스닥)
        """
        try:
            endpoint = '/api/dostk/stkinfo'
            headers = self.get_headers(api_id='ka10099', cont_yn='N', next_key='')
            
            data = {
                'mrkt_tp': str(market_type)
            }
            
            # API 요청 실행
            response = self.execute_request('POST', endpoint, data=data, headers=headers)
            
            # 응답 데이터 추출
            if not response.get('success', False):
                error_msg = response.get('error', '알 수 없는 오류')
                logger.error(f"키움증권 종목 리스트 조회 실패: {error_msg}")
                return {'output1': []}
            
            response_data = response.get('data', {})
            
            # 응답 확인 - 새로운 구조에 맞게 수정 (return_code가 문자열이 아닌 숫자로 옴)
            if 'return_code' in response_data and (response_data['return_code'] == '0' or response_data['return_code'] == 0):
                
                if 'list' in response_data and isinstance(response_data['list'], list):
                    # 리스트를 표준 형식으로 변환
                    output1 = self._convert_stock_list(response_data['list'], market_type)
                    return {'output1': output1}
                else:
                    logger.warning(f"응답에 list 필드가 없거나 예상과 다른 형식입니다")
                    return {'output1': []}
            else:
                error_msg = response_data.get('return_msg', '알 수 없는 오류')
                error_code = response_data.get('return_code', 'UNKNOWN')
                logger.error(f"키움증권 API 오류: 코드 {error_code} - {error_msg}")
                return {"output1": []}
            
        except Exception as e:
            logger.error(f"키움증권 종목 리스트 조회 실패: {str(e)}")
            return {"output1": []}
    
    def _convert_stock_list(self, stock_list, market_type):
        """종목 리스트를 표준 형식으로 변환"""
        result = []
        
        # 마켓 타입에 따른 마켓 이름
        market_name = "KOSPI" if str(market_type) == "0" else "KOSDAQ"
        
        for item in stock_list:
            # 코스피(0)인 경우 marketName이 '거래소'인 종목만 수집
            if str(market_type) == "0" and item.get('marketName') != '거래소':
                continue
                
            # 응답 데이터의 필드명에 맞게 조정
            if 'code' in item and 'name' in item:
                standard_item = {
                    'code': item['code'],
                    'name': item['name'],
                    'market': item.get('marketName', market_name)
                }
                
                # 추가 정보가 있다면 포함
                if 'lastPrice' in item:
                    standard_item['last_price'] = item['lastPrice']
                
                if 'auditInfo' in item:
                    standard_item['audit_info'] = item['auditInfo']
                
                if 'state' in item:
                    standard_item['state'] = item['state']
                
                result.append(standard_item)
        
        return result
    
    def _parse_stock_list_response(self, response_data):
        """종목 리스트 응답 파싱"""
        result = []
        
        if not response_data or not isinstance(response_data, dict):
            return result
            
        if 'list' in response_data and isinstance(response_data['list'], list):
            for item in response_data['list']:
                if isinstance(item, dict) and 'code' in item and 'name' in item:
                    result.append(item)
        
        return result
        
    def get_simple_stock_list(self, market_type):
        """시장별 종목 리스트를 간소화된 형태(코드, 이름)로 조회
        market_type: 0(코스피), 10(코스닥)
        
        Returns:
            [{'code': '000660', 'name': 'SK하이닉스'}, ...]
        """
        try:
            # 기존 API로 종목 리스트 가져오기
            response = self.get_stock_list(market_type)
            
            if not response or 'output1' not in response or not response['output1']:
                logger.warning(f"간소화된 종목 리스트 생성 실패: 원본 데이터 없음")
                return []
            
            # 코드와 이름만 추출하여 간소화된 리스트 생성
            simple_list = []
            for item in response['output1']:
                if 'code' in item and 'name' in item:
                    simple_list.append({
                        'code': item['code'],
                        'name': item['name']
                    })
            
            logger.info(f"간소화된 종목 리스트 생성 완료: {len(simple_list)}개 종목")
            return simple_list
            
        except Exception as e:
            logger.error(f"간소화된 종목 리스트 생성 실패: {str(e)}")
            return []