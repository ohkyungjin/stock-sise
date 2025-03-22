from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger


class DataCollectionScheduler:
    """데이터 수집 스케줄링 클래스"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.jobs = {}
    
    def schedule_token_refresh(self, token_manager, time='00:01'):
        """토큰 갱신 작업 스케줄링"""
        hour, minute = map(int, time.split(':'))
        job_id = f"token_refresh_{token_manager.__class__.__name__}"
        
        job = self.scheduler.add_job(
            token_manager.refresh_token,
            'cron',
            hour=hour,
            minute=minute,
            id=job_id,
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        logger.info(f"토큰 갱신 작업이 스케줄링 되었습니다: {time}")
    
    def schedule_data_collection(self, collector, func_name, time='09:00', **kwargs):
        """데이터 수집 작업 스케줄링"""
        hour, minute = map(int, time.split(':'))
        job_id = f"{collector.__class__.__name__}_{func_name}"
        
        collection_func = getattr(collector, func_name)
        
        job = self.scheduler.add_job(
            collection_func,
            'cron',
            hour=hour,
            minute=minute,
            id=job_id,
            kwargs=kwargs,
            replace_existing=True
        )
        
        self.jobs[job_id] = job
        logger.info(f"데이터 수집 작업이 스케줄링 되었습니다: {time}")
    
    def start(self):
        """스케줄러 시작"""
        if not self.scheduler.running:
            self.scheduler.start()
            logger.info("스케줄러가 시작되었습니다.")
    
    def shutdown(self):
        """스케줄러 종료"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("스케줄러가 종료되었습니다.")