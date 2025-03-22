from fastapi import FastAPI, Query, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from typing import List, Optional
import os
import json
import csv
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from loguru import logger
import sys
import uuid
import asyncio

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
logger.add("logs/api.log", level="INFO", rotation="1 day")  # API 로그 저장
logger.add("logs/error.log", level="ERROR", rotation="1 day")  # 에러 로그만 파일에 저장

# FastAPI 앱 생성
app = FastAPI(
    title="주식 시세 데이터 API",
    description="한국 주식 시세 데이터를 수집하는 API입니다. n8n과 연동하여 사용 가능합니다.",
    version="1.0.0"
)

# CORS 미들웨어 설정 (모든 오리진 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 활성 작업 상태 관리
active_tasks = {}


# 모델 정의
class DateRange(BaseModel):
    start_date: str = Field(..., description="수집 시작일 (YYYYMMDD 형식)")
    end_date: str = Field(..., description="수집 종료일 (YYYYMMDD 형식)")
    max_stocks: Optional[int] = Field(None, description="최대 처리 종목 수 (기본값: 전체 종목)")


class SingleDate(BaseModel):
    date: str = Field(..., description="수집 날짜 (YYYYMMDD 형식)")
    max_stocks: Optional[int] = Field(None, description="최대 처리 종목 수 (기본값: 전체 종목)")


class TaskResponse(BaseModel):
    task_id: str = Field(..., description="작업 ID")
    message: str = Field(..., description="메시지")
    status: str = Field(..., description="작업 상태")
    file_path: Optional[str] = Field(None, description="생성된 파일 경로")


class TaskStatus(BaseModel):
    task_id: str = Field(..., description="작업 ID")
    status: str = Field(..., description="작업 상태")
    progress: Optional[float] = Field(None, description="진행률 (0-100)")
    message: str = Field(..., description="메시지")
    file_path: Optional[str] = Field(None, description="생성된 파일 경로")
    created_at: str = Field(..., description="작업 생성 시간")
    completed_at: Optional[str] = Field(None, description="작업 완료 시간")


# 주식 종목 리스트 가져오기
async def get_stock_list():
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
        logger.critical(f"치명적 오류: {str(e)}")
        raise HTTPException(status_code=500, detail=f"토큰 발급 실패: {str(e)}")
        
    except Exception as e:
        logger.error(f"종목 리스트 가져오기 실패: {str(e)}")
        raise HTTPException(status_code=500, detail=f"종목 리스트 가져오기 실패: {str(e)}")


# 개별 종목 시세 데이터 수집 함수 (멀티프로세싱용)
def collect_price_data(stock_data, start_date, end_date):
    """개별 종목의 시세 데이터 수집"""
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


# 시세 데이터 저장 함수
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


# 날짜 검증 함수
def validate_date(date_str):
    """날짜 형식 검증 (YYYYMMDD)"""
    try:
        datetime.strptime(date_str, '%Y%m%d')
        return True
    except ValueError:
        return False


# 시세 데이터 수집 및 저장 작업 실행
async def collect_stock_prices(task_id, start_date, end_date, max_stocks=None):
    """시세 데이터 수집 및 저장 작업 실행"""
    try:
        # 작업 상태 초기화
        active_tasks[task_id] = {
            "task_id": task_id,
            "status": "processing",
            "progress": 0,
            "message": "종목 목록 가져오는 중...",
            "file_path": None,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "completed_at": None
        }
        
        # 1. 키움증권에서 종목 리스트 가져오기
        stock_list = await get_stock_list()
        
        if not stock_list:
            active_tasks[task_id]["status"] = "failed"
            active_tasks[task_id]["message"] = "종목 리스트를 가져오지 못했습니다."
            active_tasks[task_id]["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return
        
        # 작업 진행 상태 업데이트
        active_tasks[task_id]["progress"] = 10
        active_tasks[task_id]["message"] = f"종목 목록 가져오기 완료: {len(stock_list)}개 종목"
        
        # 2. 한국투자증권에서 종목별 시세 가져오기
        all_price_data = []
        success_count = 0
        fail_count = 0
        
        # 최대 처리 종목 수 설정
        if max_stocks and max_stocks > 0 and max_stocks < len(stock_list):
            logger.info(f"처음 {max_stocks}개 종목만 처리합니다.")
            stock_list = stock_list[:max_stocks]
        
        active_tasks[task_id]["message"] = f"시세 데이터 수집 시작... (총 {len(stock_list)}개 종목)"
        
        # 병렬 처리 설정
        max_workers = max(1, mp.cpu_count() - 1)  # CPU 코어 수 - 1 (최소 1)
        logger.info(f"병렬 처리 사용: {max_workers}개 프로세스")
        
        # 작업 배치 구성 (API 제한 방지)
        batch_size = 20  # 한 번에 처리할 작업 수
        wait_time = 1.0  # 배치 간 대기 시간(초)
        total_batches = (len(stock_list) + batch_size - 1) // batch_size
        
        # 모든 종목에 대해 시세 정보 수집 (병렬 처리)
        start_time = time.time()
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
                    
                    # 작업 상태 업데이트
                    progress = 10 + 80 * (completed / len(stock_list))  # 10%~90% 진행률
                    active_tasks[task_id]["progress"] = progress
                    active_tasks[task_id]["message"] = (
                        f"시세 수집 중: {completed}/{len(stock_list)} 종목 완료 "
                        f"(성공: {success_count}, 실패: {fail_count}), "
                        f"남은 시간: {estimated_remaining/60:.1f}분"
                    )
            
            # 배치 간 대기 (API 제한 방지)
            if batch_idx < total_batches - 1:
                await asyncio.sleep(wait_time)  # 배치 사이 대기
        
        total_time = time.time() - start_time
        logger.info(f"시세 데이터 수집 완료: 성공 {success_count}개, 실패 {fail_count}개, 총 {len(all_price_data)}개 데이터")
        logger.info(f"총 소요 시간: {total_time/60:.1f}분 ({total_time:.1f}초)")
        
        # 작업 상태 업데이트
        active_tasks[task_id]["progress"] = 90
        active_tasks[task_id]["message"] = f"데이터 수집 완료, CSV 파일 저장 중... ({len(all_price_data)}개 데이터)"
        
        # 3. 수집된 모든 시세 데이터를 CSV 파일로 저장
        if all_price_data:
            # 단일 날짜인 경우 파일명에 표시
            if start_date == end_date:
                date_suffix = f"{start_date}"
            else:
                date_suffix = f"{start_date}_to_{end_date}"
                
            csv_filename = f"{DATA_PATH}/stock_prices_{date_suffix}_{task_id[:8]}.csv"
            save_result = save_price_data_to_csv(all_price_data, csv_filename)
            
            if save_result:
                active_tasks[task_id]["status"] = "completed"
                active_tasks[task_id]["progress"] = 100
                active_tasks[task_id]["message"] = f"시세 데이터 수집 및 저장 완료"
                active_tasks[task_id]["file_path"] = csv_filename
            else:
                active_tasks[task_id]["status"] = "failed"
                active_tasks[task_id]["progress"] = 90
                active_tasks[task_id]["message"] = "CSV 파일 저장 실패"
        else:
            active_tasks[task_id]["status"] = "failed"
            active_tasks[task_id]["progress"] = 90
            active_tasks[task_id]["message"] = "저장할 시세 데이터가 없습니다."
            
        active_tasks[task_id]["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    except Exception as e:
        logger.error(f"시세 데이터 수집 작업 중 오류 발생: {str(e)}")
        active_tasks[task_id]["status"] = "failed"
        active_tasks[task_id]["message"] = f"오류 발생: {str(e)}"
        active_tasks[task_id]["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# 날짜 범위 수집 API
@app.post("/api/stock-prices/range", response_model=TaskResponse)
async def collect_stock_prices_by_range(
    date_range: DateRange,
    background_tasks: BackgroundTasks
):
    """특정 날짜 범위의 모든 종목 시세 데이터를 수집하는 API"""
    # 날짜 형식 검증
    if not validate_date(date_range.start_date):
        raise HTTPException(status_code=400, detail="시작일 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요.")
    
    if not validate_date(date_range.end_date):
        raise HTTPException(status_code=400, detail="종료일 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요.")
    
    # 시작일이 종료일보다 늦지 않도록 확인
    start_date_obj = datetime.strptime(date_range.start_date, '%Y%m%d')
    end_date_obj = datetime.strptime(date_range.end_date, '%Y%m%d')
    
    if start_date_obj > end_date_obj:
        raise HTTPException(status_code=400, detail=f"시작일({date_range.start_date})이 종료일({date_range.end_date})보다 늦습니다.")
    
    # 작업 ID 생성
    task_id = str(uuid.uuid4())
    
    # 백그라운드 작업 시작
    background_tasks.add_task(
        collect_stock_prices,
        task_id,
        date_range.start_date,
        date_range.end_date,
        date_range.max_stocks
    )
    
    # 응답 반환
    return {
        "task_id": task_id,
        "message": f"시세 데이터 수집 작업이 시작되었습니다. {date_range.start_date} ~ {date_range.end_date}",
        "status": "processing",
        "file_path": None
    }


# 단일 날짜 수집 API
@app.post("/api/stock-prices/date", response_model=TaskResponse)
async def collect_stock_prices_by_date(
    single_date: SingleDate,
    background_tasks: BackgroundTasks
):
    """특정 날짜의 모든 종목 시세 데이터를 수집하는 API"""
    # 날짜 형식 검증
    if not validate_date(single_date.date):
        raise HTTPException(status_code=400, detail="날짜 형식이 잘못되었습니다. YYYYMMDD 형식으로 입력해주세요.")
    
    # 작업 ID 생성
    task_id = str(uuid.uuid4())
    
    # 백그라운드 작업 시작
    background_tasks.add_task(
        collect_stock_prices,
        task_id,
        single_date.date,
        single_date.date,
        single_date.max_stocks
    )
    
    # 응답 반환
    return {
        "task_id": task_id,
        "message": f"시세 데이터 수집 작업이 시작되었습니다. 날짜: {single_date.date}",
        "status": "processing",
        "file_path": None
    }


# 작업 상태 확인 API
@app.get("/api/tasks/{task_id}", response_model=TaskStatus)
async def get_task_status(task_id: str):
    """작업 상태 확인 API"""
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail=f"작업 ID {task_id}를 찾을 수 없습니다.")
    
    return active_tasks[task_id]


# 오늘 날짜 데이터 수집 API (n8n에서 간편하게 사용 가능)
@app.get("/api/stock-prices/today", response_model=TaskResponse)
async def collect_stock_prices_today(
    background_tasks: BackgroundTasks,
    max_stocks: Optional[int] = Query(None, description="최대 처리 종목 수 (기본값: 전체 종목)")
):
    """오늘 날짜의 모든 종목 시세 데이터를 수집하는 API"""
    # 오늘 날짜 설정
    today = datetime.now().strftime('%Y%m%d')
    
    # 작업 ID 생성
    task_id = str(uuid.uuid4())
    
    # 백그라운드 작업 시작
    background_tasks.add_task(
        collect_stock_prices,
        task_id,
        today,
        today,
        max_stocks
    )
    
    # 응답 반환
    return {
        "task_id": task_id,
        "message": f"오늘({today}) 시세 데이터 수집 작업이 시작되었습니다.",
        "status": "processing",
        "file_path": None
    }


# 결과 파일 다운로드 API
@app.get("/api/download/{task_id}")
async def download_result_file(task_id: str):
    """결과 파일 다운로드 API"""
    if task_id not in active_tasks:
        raise HTTPException(status_code=404, detail=f"작업 ID {task_id}를 찾을 수 없습니다.")
    
    task = active_tasks[task_id]
    
    if task["status"] != "completed" or not task["file_path"]:
        raise HTTPException(status_code=400, detail="아직 파일이 생성되지 않았거나 작업이 실패했습니다.")
    
    file_path = task["file_path"]
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")
    
    return FileResponse(
        path=file_path,
        filename=os.path.basename(file_path),
        media_type="text/csv"
    )


# 루트 경로
@app.get("/")
async def root():
    """API 서버가 실행 중인지 확인하는 엔드포인트"""
    return {"message": "주식 시세 데이터 API 서버가 실행 중입니다."}


# FASTAPI 서버 실행 코드
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main_api:app", host="0.0.0.0", port=8000, reload=True) 