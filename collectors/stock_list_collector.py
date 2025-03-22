import json
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
from loguru import logger

from config.settings import DATA_PATH


class StockListCollector:
    """주식 종목 리스트 수집 클래스"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.data_path = DATA_PATH / 'stock_list'
        self.data_path.mkdir(exist_ok=True, parents=True)
    
    def collect(self, market_types=None):
        """시장 유형별 종목 목록 수집"""
        if market_types is None:
            market_types = [0, 10]  # 기본값: 코스피(0), 코스닥(10)
        
        all_stocks = []
        
        for market_type in market_types:
            logger.info(f"시장 {market_type} 종목 목록 수집 시작...")
            
            try:
                # API 클라이언트로 종목 목록 조회
                raw_data = self.api_client.get_stock_list(market_type)
                
                if raw_data:
                    # 응답 데이터 로깅 (디버깅용)
                    logger.debug(f"API 응답: {json.dumps(raw_data, ensure_ascii=False)[:300]}...")
                    
                    # 데이터 표준화
                    stocks = self.standardize_data(raw_data, market_type)
                    
                    if stocks:
                        logger.info(f"시장 {market_type} 종목 {len(stocks)}개 수집 완료")
                        all_stocks.extend(stocks)
                    else:
                        logger.warning(f"시장 {market_type} 종목 목록이 비어있습니다")
                else:
                    logger.error(f"시장 {market_type} 종목 목록 API 호출 실패")
            
            except Exception as e:
                logger.error(f"시장 {market_type} 종목 목록 수집 중 오류 발생: {str(e)}")
        
        # 중복 제거
        unique_stocks = []
        unique_codes = set()
        
        for stock in all_stocks:
            if stock['code'] not in unique_codes:
                unique_codes.add(stock['code'])
                unique_stocks.append(stock)
        
        logger.info(f"전체 {len(unique_stocks)}개 종목 수집 완료 (중복 제거 후)")
        
        # 수집된 종목이 없으면 빈 리스트 반환 (기본 종목 목록 사용 안함)
        if not unique_stocks:
            logger.warning("종목 수집 결과가 없습니다. 빈 리스트를 반환합니다.")
        
        return unique_stocks
    
    def _load_latest_stock_list(self):
        """가장 최근에 저장된 종목 리스트 파일 로드"""
        try:
            # 최신 파일 확인
            latest_file = self.data_path / "stock_list_latest.json"
            if latest_file.exists():
                with open(latest_file, 'r', encoding='utf-8') as f:
                    stocks = json.load(f)
                logger.info(f"최신 종목 리스트 파일 로드 성공: {latest_file}")
                return stocks
            
            # 없으면 가장 최근 날짜 파일 찾기
            json_files = list(self.data_path.glob("stock_list_*.json"))
            if json_files:
                latest_file = max(json_files, key=lambda x: x.stat().st_mtime)
                with open(latest_file, 'r', encoding='utf-8') as f:
                    stocks = json.load(f)
                logger.info(f"최근 종목 리스트 파일 로드 성공: {latest_file}")
                return stocks
        
        except Exception as e:
            logger.error(f"저장된 종목 리스트 로드 실패: {str(e)}")
        
        return []
    
    def standardize_data(self, api_response, market_type):
        """API 응답 데이터를 표준 형식으로 변환"""
        try:
            standardized_data = []
            
            # 응답에 list 필드가 없으면 빈 리스트 반환
            if 'list' not in api_response:
                logger.error(f"API 응답에 'list' 필드가 없습니다: {api_response}")
                return []
            
            stock_list = api_response['list']
            
            # 코스피(거래소)인 경우, marketName이 '거래소'인 종목만 필터링
            if market_type == 0:  # 코스피
                logger.info("코스피(거래소) 종목만 필터링합니다.")
                filtered_list = [
                    {'code': item['code'], 'name': item['name']} 
                    for item in stock_list 
                    if item.get('marketName') == '거래소'
                ]
            # 코스닥인 경우 모든 종목 코드 수집
            else:
                logger.info("코스닥 종목 코드를 수집합니다.")
                filtered_list = [
                    {'code': item['code'], 'name': item['name']} 
                    for item in stock_list
                ]
            
            logger.info(f"수집된 종목 수: {len(filtered_list)}")
            return filtered_list
            
        except Exception as e:
            logger.error(f"데이터 표준화 중 오류 발생: {str(e)}")
            logger.debug(f"원본 API 응답: {api_response}")
            return []
    
    def save_data(self, data):
        """데이터 파일 저장"""
        today = datetime.now().strftime('%Y%m%d')
        file_path = self.data_path / f"stock_list_{today}.json"
        
        try:
            # JSON 형식으로 저장
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            # CSV 형식으로도 저장
            csv_path = self.data_path / f"stock_list_{today}.csv"
            pd.DataFrame(data).to_csv(csv_path, index=False, encoding='utf-8-sig')
            
            logger.info(f"종목 리스트 저장 완료: {file_path}")
            logger.info(f"종목 리스트 CSV 저장 완료: {csv_path}")
            
            # 최신 데이터 링크 생성
            latest_json = self.data_path / "stock_list_latest.json"
            latest_csv = self.data_path / "stock_list_latest.csv"
            
            if latest_json.exists():
                latest_json.unlink()
            if latest_csv.exists():
                latest_csv.unlink()
            
            try:
                latest_json.symlink_to(file_path.name)
                latest_csv.symlink_to(csv_path.name)
            except Exception as e:
                # 심볼릭 링크 생성 실패 시 파일 복사로 대체
                logger.warning(f"심볼릭 링크 생성 실패, 파일 복사로 대체: {str(e)}")
                import shutil
                shutil.copy2(file_path, latest_json)
                shutil.copy2(csv_path, latest_csv)
            
        except Exception as e:
            logger.error(f"데이터 저장 중 오류 발생: {str(e)}")