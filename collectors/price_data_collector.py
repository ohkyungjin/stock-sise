import json
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
from loguru import logger
import traceback

from config.settings import DATA_PATH


class PriceDataCollector:
    """주가 데이터 수집 클래스"""
    
    def __init__(self, api_client):
        self.api_client = api_client
        self.data_path = DATA_PATH / 'price_data'
        self.data_path.mkdir(exist_ok=True, parents=True)
        # 종목명 사전 (코드 -> 종목명 매핑)
        self.stock_names = {}
        self._load_stock_names()
    
    def _load_stock_names(self):
        """종목 리스트 파일에서 종목명 정보 로드"""
        # 종목 리스트 파일 경로들을 확인
        stock_list_paths = [
            DATA_PATH / 'stock_list' / 'stock_list_latest.json',
            DATA_PATH / 'stock_list' / 'stock_list_latest.csv'
        ]
        
        # 최신 파일이 없으면 가장 최근 파일 찾기
        if not any(path.exists() for path in stock_list_paths):
            try:
                stock_list_dir = DATA_PATH / 'stock_list'
                if stock_list_dir.exists():
                    # 날짜 패턴(YYYYMMDD)으로 된 파일들 중 가장 최신 파일 찾기
                    json_files = list(stock_list_dir.glob('stock_list_*.json'))
                    if json_files:
                        # 파일명 기준 내림차순 정렬
                        latest_file = sorted(json_files, reverse=True)[0]
                        stock_list_paths.append(latest_file)
            except Exception as e:
                logger.error(f"최신 종목 리스트 파일 검색 중 오류: {str(e)}")

        # 하드코딩된 주요 종목 정보 추가 (기본값)
        self.stock_names.update({
            '005930': '삼성전자',
            '000660': 'SK하이닉스',
            '035420': 'NAVER',
            '035720': '카카오',
            '051910': 'LG화학',
            '207940': '삼성바이오로직스',
            '006400': '삼성SDI',
            '005380': '현대차',
            '000270': '기아',
            '373220': 'LG에너지솔루션'
        })
        
        # 종목 리스트 파일에서 정보 로드 시도
        for path in stock_list_paths:
            if not path.exists():
                continue
                
            try:
                if str(path).endswith('.json'):
                    with open(path, 'r', encoding='utf-8') as f:
                        stock_list = json.load(f)
                    
                    # JSON 형식에 따라 매핑 생성
                    if isinstance(stock_list, list):
                        for stock in stock_list:
                            if isinstance(stock, dict):
                                # 다양한 필드명 형식 지원
                                code = stock.get('code') or stock.get('종목코드')
                                name = stock.get('name') or stock.get('종목명')
                                if code and name:
                                    self.stock_names[code] = name
                
                elif str(path).endswith('.csv'):
                    df = pd.read_csv(path, encoding='utf-8-sig')
                    
                    # CSV 컬럼명 확인
                    code_col = None
                    name_col = None
                    
                    for col in df.columns:
                        if col.lower() in ['code', '종목코드', 'stock_code']:
                            code_col = col
                        elif col.lower() in ['name', '종목명', 'stock_name']:
                            name_col = col
                    
                    if code_col and name_col:
                        # 데이터프레임에서 종목코드-종목명 매핑 생성
                        for _, row in df.iterrows():
                            code = str(row[code_col]).strip()
                            # 종목코드 형식 보정 (숫자로만 구성된 코드라면 6자리 맞추기)
                            if code.isdigit():
                                code = code.zfill(6)
                            self.stock_names[code] = row[name_col]
                
                # 파일 하나만 성공적으로 로드되면 중단
                break
                
            except Exception as e:
                logger.error(f"종목명 정보 로드 중 오류 ({path}): {str(e)}")
        
    
    def get_stock_name(self, stock_code):
        """종목 코드에 해당하는 종목명 반환"""
        # 종목코드 형식 보정 (숫자로만 구성된 코드라면 6자리 맞추기)
        if stock_code.isdigit():
            stock_code = stock_code.zfill(6)
        
        # 종목명 사전에 없는 경우, 필요시 API로부터 가져오기 시도
        if stock_code not in self.stock_names:
            try:
                # API를 통해 종목명 가져오기 시도 (구현 필요)
                # 예: name = self.api_client.get_stock_name(stock_code)
                # self.stock_names[stock_code] = name
                
                # API 구현이 없는 경우 종목코드 뒤에 '(주)' 추가하여 임시 이름 생성
                self.stock_names[stock_code] = f"{stock_code}(주)"
                logger.warning(f"종목 {stock_code}의 종목명 정보가 없어 임시 이름을 사용합니다.")
            except Exception as e:
                logger.error(f"종목명 조회 실패 ({stock_code}): {str(e)}")
                return f"{stock_code}"  # 실패 시 종목코드 그대로 반환
        
        return self.stock_names[stock_code]
    
    def collect(self, stock_code, period='D', is_adjusted=True, start_date=None, end_date=None, max_count=None, save_to_file=False):
        """특정 종목의 주가 데이터 수집"""
        try:
            # 시작일과 종료일 처리
            start_date_str = self._format_date(start_date) if start_date else None
            end_date_str = self._format_date(end_date) if end_date else self._format_date(datetime.now())
            
            # API 호출하여 주가 데이터 가져오기
            raw_data = self.api_client.get_price_data(
                stock_code=stock_code,
                period=period,
                is_adjusted=is_adjusted,
                start_date=start_date_str,
                end_date=end_date_str,
                max_count=max_count
            )
            
            # 데이터 표준화
            price_data = self.standardize_data(raw_data)
            
            # 개별 파일 저장 옵션이 활성화된 경우에만 파일로 저장
            if save_to_file and price_data:
                self.save_individual_file({stock_code: price_data})
            
            return price_data
            
        except Exception as e:
            logger.error(f"'{stock_code}' 종목 주가 데이터 수집 실패: {str(e)}")
            logger.debug(f"상세 오류: {traceback.format_exc()}")
            return []
    
    def collect_multiple(self, stock_codes, period='D', is_adjusted=True):
        """여러 종목의 주가 데이터 수집"""
        results = {}
        
        for stock_code in stock_codes:
            try:
                price_data = self.collect(
                    stock_code=stock_code,
                    period=period,
                    is_adjusted=is_adjusted
                )
                results[stock_code] = price_data
            except Exception as e:
                logger.error(f"종목 {stock_code} 주가 데이터 수집 실패: {str(e)}")
                results[stock_code] = []
        
        return results
    
    def standardize_data(self, raw_data, stock_code, stock_name=None):
        """수집된 주가 데이터 표준화"""
        price_data = []
        
        if stock_name is None:
            stock_name = self.get_stock_name(stock_code)
        
        # output1에서 추가 정보 추출
        additional_info = {}
        if 'output1' in raw_data and isinstance(raw_data['output1'], dict):
            additional_info = raw_data['output1']
            
            # 종목명이 API 응답에 있으면 업데이트
            if 'hts_kor_isnm' in additional_info and additional_info['hts_kor_isnm']:
                stock_name = additional_info['hts_kor_isnm']
                # 종목명 사전에 추가
                self.stock_names[stock_code] = stock_name
        
        try:
            # API 응답 구조에 따라 데이터 추출 (output2가 차트 데이터)
            if 'output2' in raw_data and isinstance(raw_data['output2'], list):
                items = raw_data['output2']
            else:
                logger.warning("주가 데이터 형식이 예상과 다릅니다")
                return []
            
            for item in items:
                # 새로운 API 응답 구조에 맞게 조정
                try:
                    # 데이터 변환
                    price = {
                        '종목코드': stock_code,
                        '종목명': stock_name,
                        '거래일': item.get('stck_bsop_date', ''),  # 거래일자
                        '시가': int(item.get('stck_oprc', 0)),     # 시가
                        '고가': int(item.get('stck_hgpr', 0)),     # 고가
                        '저가': int(item.get('stck_lwpr', 0)),     # 저가
                        '종가': int(item.get('stck_clpr', 0)),     # 종가
                        '거래량': int(item.get('acml_vol', 0)),    # 거래량
                        '거래대금': int(item.get('acml_tr_pbmn', 0) or 0),  # 거래대금
                        '등락률': float(item.get('prdy_ctrt', 0) if item.get('prdy_ctrt') else 0),  # 등락률
                        '수정주가여부': '수정' if item.get('prtt_rate', '1.00') != '1.00' else '원주가'  # 수정주가 여부
                    }
                    
                    # 회전율 - 별도로 계산 또는 추가 정보에서 가져옴
                    if 'vol_tnrt' in additional_info:
                        price['회전율'] = float(additional_info.get('vol_tnrt', 0) or 0)
                    else:
                        price['회전율'] = 0.0
                    
                    price_data.append(price)
                except Exception as e:
                    logger.warning(f"데이터 변환 중 오류 발생 (항목 건너뜀): {str(e)}")
            
            # 날짜 기준으로 오름차순 정렬
            price_data.sort(key=lambda x: x['거래일'])
            
        except Exception as e:
            logger.error(f"주가 데이터 표준화 중 오류 발생: {str(e)}")
        
        return price_data
    
    def save_data(self, data, stock_code, stock_name=None, period='D', file_suffix=""):
        """주가 데이터 파일 저장 (CSV 파일만 생성)"""
        if not data:
            logger.warning(f"저장할 주가 데이터가 없습니다: {stock_code}")
            return
        
        if stock_name is None:
            stock_name = self.get_stock_name(stock_code)
        
        # 종목 코드별 디렉토리 생성
        stock_dir = self.data_path / stock_code
        stock_dir.mkdir(exist_ok=True, parents=True)
        
        # 파일명 생성
        period_name = {'D': '일봉', 'W': '주봉', 'M': '월봉'}.get(period, '일봉')
        
        # 기간 정보가 있으면 파일명에 포함
        csv_filename = f"{stock_code}_{stock_name}_{period_name}{file_suffix}.csv"
        csv_path = stock_dir / csv_filename
        
        try:
            # 데이터프레임 생성
            df = pd.DataFrame(data)
            
            # CSV로 저장
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            return csv_path
        
        except Exception as e:
            logger.error(f"주가 데이터 저장 중 오류 발생: {str(e)}")
            return None

    def save_to_file(self, data_dict, file_format='csv'):
        """수집한 주가 데이터를 단일 파일로 저장"""
        if not data_dict:
            logger.warning("저장할 주가 데이터가 없습니다.")
            return None

        # 저장 디렉토리 생성
        save_dir = self.data_path / 'price_data'
        save_dir.mkdir(parents=True, exist_ok=True)
        
        current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if file_format.lower() == 'csv':
            # 모든 종목 데이터를 하나의 DataFrame으로 통합
            all_data = []
            
            for stock_code, stock_data in data_dict.items():
                if not stock_data or not isinstance(stock_data, list):
                    logger.warning(f"종목 {stock_code}의 데이터가 비어있거나 잘못된 형식입니다.")
                    continue
                    
                # 각 종목 데이터에 종목코드 컬럼 추가
                for item in stock_data:
                    item['종목코드'] = stock_code
                
                all_data.extend(stock_data)
            
            if not all_data:
                logger.warning("저장할 데이터가 없습니다.")
                return None
                
            # 통합 DataFrame 생성
            df = pd.DataFrame(all_data)
            
            # 필요한 경우 날짜 컬럼을 datetime 타입으로 변환
            if '날짜' in df.columns:
                df['날짜'] = pd.to_datetime(df['날짜'])
                # 날짜를 인덱스로 설정하지 않고 일반 컬럼으로 유지
            
            # 파일명 생성 (날짜 기반)
            file_path = save_dir / f'all_price_data_{current_time}.csv'
            
            # CSV 파일로 저장
            df.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            return file_path
            
        elif file_format.lower() == 'json':
            # JSON 파일로 저장
            file_path = save_dir / f'all_price_data_{current_time}.json'
            
            # 종목코드를 포함하는 형태로 데이터 구조 변경
            formatted_data = []
            for stock_code, stock_data in data_dict.items():
                if not stock_data or not isinstance(stock_data, list):
                    continue
                    
                for item in stock_data:
                    item['종목코드'] = stock_code
                    formatted_data.append(item)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(formatted_data, f, ensure_ascii=False, indent=2)
            
            return file_path
            
        else:
            logger.error(f"지원하지 않는 파일 형식입니다: {file_format}")
            return None

    def save_individual_file(self, data_dict, file_format='csv'):
        """개별 종목 데이터를 파일로 저장 (기존 메서드 유지)"""
        if not data_dict:
            logger.warning("저장할 주가 데이터가 없습니다.")
            return None

        # 저장 디렉토리 생성
        save_dir = self.data_path / 'price_data'
        save_dir.mkdir(parents=True, exist_ok=True)
        
        saved_files = []
        
        for stock_code, stock_data in data_dict.items():
            if not stock_data:
                logger.warning(f"종목 {stock_code}의 저장할 데이터가 없습니다.")
                continue
            
            if file_format.lower() == 'csv':
                # CSV 파일로 저장
                df = pd.DataFrame(stock_data)
                
                # 날짜 컬럼을 datetime 타입으로 변환
                if '날짜' in df.columns:
                    df['날짜'] = pd.to_datetime(df['날짜'])
                    df.set_index('날짜', inplace=True)
                    df.sort_index(inplace=True)
                
                file_path = save_dir / f'{stock_code}_price.csv'
                df.to_csv(file_path, encoding='utf-8-sig')
                saved_files.append(file_path)
            
            elif file_format.lower() == 'json':
                # JSON 파일로 저장
                file_path = save_dir / f'{stock_code}_price.json'
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(stock_data, f, ensure_ascii=False, indent=2)
                saved_files.append(file_path)
            
            else:
                logger.error(f"지원하지 않는 파일 형식입니다: {file_format}")
        
        return saved_files if saved_files else None