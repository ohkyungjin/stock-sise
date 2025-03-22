import os
import sys
import json
import csv
import time
import argparse
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from datetime import datetime, timedelta
from loguru import logger

# 환경 설정 및 로깅 설정
from config.settings import (
    KIWOOM_APP_KEY, KIWOOM_APP_SECRET,
    KIS_APP_KEY, KIS_APP_SECRET,
    DATA_PATH
)

# API 클라이언트 및 토큰 관리
from api.token_manager import KiwoomTokenManager, KoreaInvestmentTokenManager, TokenFailedException
from api.kiwoom_client import KiwoomAPIClient
from api.korea_investment_client import KoreaInvestmentAPIClient
from utils.error_handler import APIErrorHandler


# 간단한 로깅 설정
logger.remove()  # 기본 핸들러 제거
logger.add(sys.stderr, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>")  # 콘솔 출력
logger.add("logs/error.log", level="ERROR", rotation="1 day")  # 에러 로그만 파일에 저장


def print_progress_bar(progress, total, prefix='', suffix='', length=50, fill='█', print_end='\r'):
    """프로그레스 바 출력 함수"""
    percent = f"{100 * (progress / float(total)):.1f}"
    filled_length = int(length * progress // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    progress_line = f"\r{prefix} |{bar}| {progress}/{total} ({percent}%) {suffix}"
    
    # 너무 긴 경우 터미널 폭에 맞게 자르기
    terminal_width = 100  # 임의의 폭 (Windows에서는 os.get_terminal_size()가 문제될 수 있음)
    if len(progress_line) > terminal_width:
        progress_line = progress_line[:terminal_width-3] + "..."
    
    print(progress_line, end=print_end)
    
    # 완료시 줄바꿈
    if progress >= total: 
        print()


def get_stock_list_from_kiwoom():
    """키움증권 API를 통해 종목 리스트 가져오기"""
    logger.info("키움증권에서 종목 리스트 가져오기 시작")
    
    try:
        # 토큰 매니저 생성 - 실패 시 예외 발생
        token_manager = KiwoomTokenManager(KIWOOM_APP_KEY, KIWOOM_APP_SECRET)
        
        # API 클라이언트 생성
        client = KiwoomAPIClient(token_manager)
        
        # 에러 핸들러 생성
        error_handler = APIErrorHandler()
        
        # 코스피 종목 리스트 가져오기 (marketName이 '거래소'인 것만)
        logger.info("코스피 종목 조회 중...")
        kospi_stocks = error_handler.handle_request(client.get_simple_stock_list, 0)
        
        # 코스닥 종목 리스트 가져오기
        logger.info("코스닥 종목 조회 중...")
        kosdaq_stocks = error_handler.handle_request(client.get_simple_stock_list, 10)
        
        # 결과 합치기
        all_stocks = []
        
        if kospi_stocks and isinstance(kospi_stocks, list):
            # 코스피 종목 표시
            for stock in kospi_stocks:
                stock['market'] = 'KOSPI'
            all_stocks.extend(kospi_stocks)
            logger.info(f"코스피 종목 {len(kospi_stocks)}개 추가")
        else:
            logger.warning("코스피 종목 데이터를 가져오지 못했습니다")
        
        if kosdaq_stocks and isinstance(kosdaq_stocks, list):
            # 코스닥 종목 표시
            for stock in kosdaq_stocks:
                stock['market'] = 'KOSDAQ'
            all_stocks.extend(kosdaq_stocks)
            logger.info(f"코스닥 종목 {len(kosdaq_stocks)}개 추가")
        else:
            logger.warning("코스닥 종목 데이터를 가져오지 못했습니다")
        
        if not all_stocks:
            logger.error("종목 정보를 가져오지 못했습니다")
            return []
            
        logger.info(f"총 {len(all_stocks)}개 종목 정보 가져오기 완료")
        return all_stocks
        
    except TokenFailedException as e:
        # 토큰 발급 실패 시 프로그램 종료
        logger.critical(f"치명적 오류: {str(e)}")
        sys.exit(1)  # 비정상 종료 코드로 프로그램 종료
        
    except Exception as e:
        logger.error(f"종목 리스트 가져오기 실패: {str(e)}")
        return []


def collect_price_data(stock_data, start_date, end_date):
    """개별 종목의 시세 데이터 수집 (멀티프로세싱용)"""
    stock_code = stock_data.get('code')
    stock_name = stock_data.get('name')
    
    if not stock_code:
        return None
    
    try:
        # 토큰 매니저 생성
        token_manager = KoreaInvestmentTokenManager(KIS_APP_KEY, KIS_APP_SECRET)
        
        # API 클라이언트 생성
        client = KoreaInvestmentAPIClient(token_manager)
        
        # 에러 핸들러 생성
        error_handler = APIErrorHandler()
        
        # 시세 정보 가져오기
        price_data = error_handler.handle_request(
            client.get_daily_price,
            stock_code=stock_code,
            period='D',
            is_adjusted=True,
            start_date=start_date,
            end_date=end_date
        )
        
        # 결과 검증 및 처리
        if 'error' in price_data:
            return None
            
        # 결과 처리
        result = []
        if 'output2' in price_data and isinstance(price_data['output2'], list):
            # 각 항목에 종목코드와 종목명 추가
            for item in price_data['output2']:
                item['종목코드'] = stock_code
                item['종목명'] = stock_name or ''
            result = price_data['output2']
            return result
        else:
            return None
            
    except Exception:
        return None


def get_stock_price_from_kis(stock_code, stock_name=None, start_date=None, end_date=None):
    """한국투자증권 API를 통해 종목별 시세 가져오기"""
    # 상세 로그는 디버그 레벨로 변경
    logger.debug(f"종목 {stock_code}({stock_name}) 시세 요청 중...")
    
    try:
        # 입력값 검증
        if not stock_code or not isinstance(stock_code, str):
            logger.error(f"유효하지 않은 종목코드: {stock_code}")
            return []
            
        # 토큰 매니저 생성 - 실패 시 예외 발생
        token_manager = KoreaInvestmentTokenManager(KIS_APP_KEY, KIS_APP_SECRET)
        
        # API 클라이언트 생성
        client = KoreaInvestmentAPIClient(token_manager)
        
        # 에러 핸들러 생성
        error_handler = APIErrorHandler()
        
        # 시세 정보 가져오기
        price_data = error_handler.handle_request(
            client.get_daily_price,
            stock_code=stock_code,
            period='D',  # 일별 데이터
            is_adjusted=True,  # 수정주가 적용
            start_date=start_date,
            end_date=end_date
        )
        
        # 결과 검증 및 처리
        if 'error' in price_data:
            logger.error(f"API 오류 ({stock_code}): {price_data['error']}")
            return []
            
        # 결과 처리
        result = []
        if 'output2' in price_data and isinstance(price_data['output2'], list):
            # 각 항목에 종목코드와 종목명 추가
            for item in price_data['output2']:
                item['종목코드'] = stock_code
                item['종목명'] = stock_name or ''
            result = price_data['output2']
            # 상세 로그는 디버그 레벨로 변경
            logger.debug(f"종목 {stock_code} 시세 {len(result)}개 데이터 조회 완료")
        else:
            logger.debug(f"종목 {stock_code} 시세 데이터가 없습니다")
        
        return result
        
    except TokenFailedException as e:
        # 토큰 발급 실패 시 프로그램 종료
        logger.critical(f"치명적 오류: {str(e)}")
        sys.exit(1)  # 비정상 종료 코드로 프로그램 종료
        
    except Exception as e:
        logger.error(f"종목 {stock_code} 시세 데이터 가져오기 실패: {str(e)}")
        return []


def save_price_data_to_csv(price_data, filename):
    """시세 데이터를 CSV 파일로 저장"""
    if not price_data:
        logger.error("저장할 시세 데이터가 없습니다.")
        return False
        
    try:
        # 디렉토리 생성
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        # 필드명 매핑 (영어 -> 한글)
        field_mapping = {
            'stck_bsop_date': '날짜',
            'stck_clpr': '종가',
            'stck_oprc': '시가',
            'stck_hgpr': '고가',
            'stck_lwpr': '저가',
            'acml_vol': '거래량',
            'acml_tr_pbmn': '거래대금',
            'flng_cls_code': '락구분',
            'prtt_rate': '분할비율',
            '종목코드': '종목코드',
            '종목명': '종목명'
        }
        
        # 필드 순서 정의
        field_order = [
            '종목코드', '종목명', '날짜', '시가', '고가', 
            '저가', '종가', '거래량', '거래대금', '락구분', '분할비율'
        ]
        
        # CSV 파일로 저장
        with open(filename, 'w', encoding='utf-8-sig', newline='') as f:
            # 헤더 준비
            writer = csv.DictWriter(f, fieldnames=field_order)
            writer.writeheader()
            
            # 데이터 변환 및 저장
            for item in price_data:
                # 영어 필드명을 한글로 변환
                korean_item = {}
                for eng_key, value in item.items():
                    if eng_key in field_mapping:
                        korean_key = field_mapping[eng_key]
                        korean_item[korean_key] = value
                
                # 변환된 데이터 쓰기
                writer.writerow(korean_item)
        
        logger.info(f"시세 데이터 CSV 저장 완료: {filename} ({len(price_data)}개 항목)")
        return True
    except Exception as e:
        logger.error(f"CSV 저장 실패: {str(e)}")
        return False


def parse_arguments():
    """명령줄 인수 파싱"""
    parser = argparse.ArgumentParser(description='주식 시세 데이터 수집 프로그램')
    
    parser.add_argument('--start-date', type=str, 
                        help='수집 시작일 (YYYYMMDD 형식, 기본값: 30일 전)')
    
    parser.add_argument('--end-date', type=str, 
                        help='수집 종료일 (YYYYMMDD 형식, 기본값: 오늘)')
    
    parser.add_argument('--date', type=str, 
                        help='단일 날짜 수집 (YYYYMMDD 형식, --start-date와 --end-date보다 우선)')
    
    parser.add_argument('--max-stocks', type=int, default=None,
                        help='최대 처리 종목 수 (기본값: 전체 종목)')
    
    parser.add_argument('--batch-size', type=int, default=20,
                        help='배치당 처리 종목 수 (기본값: 20)')
    
    parser.add_argument('--wait-time', type=float, default=1.0,
                        help='배치 간 대기 시간(초) (기본값: 1.0)')
    
    return parser.parse_args()


def main():
    """메인 함수"""
    try:
        # 명령줄 인수 파싱
        args = parse_arguments()
        
        logger.info("주식 시세 데이터 수집 프로그램 시작")
        
        # 날짜 설정
        today = datetime.now().strftime('%Y%m%d')
        default_start_date = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
        
        # 단일 날짜 인수가 있으면 시작일과 종료일로 설정
        if args.date:
            try:
                # 날짜 형식 검증
                datetime.strptime(args.date, '%Y%m%d')
                start_date = args.date
                end_date = args.date
                logger.info(f"수집 날짜: {args.date}")
            except ValueError:
                logger.error(f"잘못된 날짜 형식: {args.date} (YYYYMMDD 형식으로 입력해주세요)")
                return
        else:
            # 시작일 설정
            if args.start_date:
                try:
                    # 날짜 형식 검증
                    datetime.strptime(args.start_date, '%Y%m%d')
                    start_date = args.start_date
                except ValueError:
                    logger.error(f"잘못된 시작일 형식: {args.start_date} (YYYYMMDD 형식으로 입력해주세요)")
                    return
            else:
                start_date = default_start_date
            
            # 종료일 설정
            if args.end_date:
                try:
                    # 날짜 형식 검증
                    datetime.strptime(args.end_date, '%Y%m%d')
                    end_date = args.end_date
                except ValueError:
                    logger.error(f"잘못된 종료일 형식: {args.end_date} (YYYYMMDD 형식으로 입력해주세요)")
                    return
            else:
                end_date = today
        
        # 날짜 유효성 검사 (시작일이 종료일보다 늦지 않도록)
        if datetime.strptime(start_date, '%Y%m%d') > datetime.strptime(end_date, '%Y%m%d'):
            logger.error(f"시작일({start_date})이 종료일({end_date})보다 늦습니다.")
            return
        
        logger.info(f"수집 기간: {start_date} ~ {end_date}")
        
        # 1. 키움증권에서 종목 리스트 가져오기
        stock_list = get_stock_list_from_kiwoom()
        
        if not stock_list:
            logger.error("종목 리스트를 가져오지 못했습니다. 프로그램을 종료합니다.")
            return
        
        # 2. 한국투자증권에서 종목별 시세 가져오기
        all_price_data = []
        success_count = 0
        fail_count = 0
        
        logger.info(f"총 {len(stock_list)}개 종목의 시세 데이터 수집 시작")
        
        # 최대 처리 종목 수 설정
        max_stocks = args.max_stocks
        
        # 병렬 처리 설정
        max_workers = mp.cpu_count() - 1  # CPU 코어 수 - 1 (최소 1)
        max_workers = max(1, max_workers)
        logger.info(f"병렬 처리 사용: {max_workers}개 프로세스")
        
        # 처리할 종목 제한
        if max_stocks:
            if len(stock_list) > max_stocks:
                logger.warning(f"종목 리스트가 너무 많습니다. 처음 {max_stocks}개만 처리합니다.")
                stock_list = stock_list[:max_stocks]
        
        # 작업 배치 구성 (API 제한 방지)
        batch_size = args.batch_size
        wait_time = args.wait_time
        total_batches = (len(stock_list) + batch_size - 1) // batch_size
        
        # 모든 종목에 대해 시세 정보 수집 (병렬 처리)
        start_time = time.time()
        
        # 프로그레스바 초기화
        print_progress_bar(0, len(stock_list), prefix='진행률:', suffix='', length=40)
        
        completed = 0
        
        # 배치 단위로 처리
        for batch_idx in range(total_batches):
            batch_start = batch_idx * batch_size
            batch_end = min(batch_start + batch_size, len(stock_list))
            current_batch = stock_list[batch_start:batch_end]
            
            # 병렬 처리 준비
            collect_with_dates = partial(collect_price_data, start_date=start_date, end_date=end_date)
            
            # 병렬 실행
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(collect_with_dates, stock): stock for stock in current_batch}
                
                for future in as_completed(futures):
                    stock = futures[future]
                    try:
                        price_data = future.result()
                        if price_data:
                            all_price_data.extend(price_data)
                            success_count += 1
                        else:
                            fail_count += 1
                    except Exception as e:
                        logger.error(f"종목 {stock.get('code')} 데이터 수집 중 오류: {str(e)}")
                        fail_count += 1
                    
                    # 진행 상황 업데이트
                    completed += 1
                    elapsed = time.time() - start_time
                    estimated_total = elapsed / completed * len(stock_list)
                    estimated_remaining = estimated_total - elapsed
                    
                    # 예상 종료 시간 계산
                    end_time = datetime.now() + timedelta(seconds=estimated_remaining)
                    end_time_str = end_time.strftime("%H:%M:%S")
                    
                    # 진행률 표시
                    suffix = f"남은 시간: {estimated_remaining/60:.1f}분 | 예상 종료: {end_time_str}"
                    print_progress_bar(completed, len(stock_list), prefix='진행률:', suffix=suffix, length=40)
            
            # 배치 간 대기 (API 제한 방지)
            if batch_idx < total_batches - 1:
                time.sleep(wait_time)  # 배치 사이 대기
        
        total_time = time.time() - start_time
        logger.info(f"\n시세 데이터 수집 완료: 성공 {success_count}개, 실패 {fail_count}개, 총 {len(all_price_data)}개 데이터")
        logger.info(f"총 소요 시간: {total_time/60:.1f}분 ({total_time:.1f}초)")
        
        # 3. 수집된 모든 시세 데이터를 CSV 파일로 저장
        if all_price_data:
            # 단일 날짜인 경우 파일명에 표시
            if start_date == end_date:
                date_suffix = f"{start_date}"
            else:
                date_suffix = f"{start_date}_to_{end_date}"
                
            csv_filename = f"{DATA_PATH}/stock_prices_{date_suffix}.csv"
            save_price_data_to_csv(all_price_data, csv_filename)
            logger.info(f"모든 주식 시세 데이터 저장 완료: {csv_filename}")
        else:
            logger.error("저장할 시세 데이터가 없습니다.")
        
        logger.info("프로그램 정상 종료")
    
    except TokenFailedException as e:
        # 토큰 발급 실패 시 프로그램 종료
        logger.critical(f"치명적 오류: {str(e)}")
        sys.exit(1)  # 비정상 종료 코드로 프로그램 종료
        
    except Exception as e:
        logger.error(f"프로그램 실행 중 오류 발생: {str(e)}")
        sys.exit(1)  # 비정상 종료


if __name__ == "__main__":
    main()
