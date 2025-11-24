import os
import logging
from typing import List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn
from audio_fingerprint_processor import AudioFingerprintProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Audio Fingerprint Processor API",
    description="API for processing audio fingerprints using Chromaprint/AcoustID",
    version="1.0.0"
)

# Global processor instance
processor = None

def get_processor():
    global processor
    if processor is None:
        # Initialize with default settings, can be tuned via env vars
        max_workers = int(os.environ.get("MAX_WORKERS", "4"))
        processor = AudioFingerprintProcessor(max_workers=max_workers)
        try:
            processor.ensure_table_exists()
        except Exception as e:
            logger.error(f"Failed to initialize table: {e}")
    return processor

class ProcessSourceRequest(BaseModel):
    source: str
    retry_errors: bool = False

class ProcessAssetsRequest(BaseModel):
    asset_ids: List[str]

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up Audio Fingerprint Processor API...")
    get_processor()

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.get("/stats")
async def get_stats():
    proc = get_processor()
    return proc.get_processing_stats()

def run_processing_job(source: str, retry_errors: bool):
    logger.info(f"Starting background job for source: {source}")
    proc = get_processor()
    try:
        assets = proc.get_all_assets_by_source(source, retry_errors=retry_errors)
        if assets:
            proc.process_assets_parallel(assets, is_retry=retry_errors)
        else:
            logger.info(f"No assets found for source: {source}")
    except Exception as e:
        logger.error(f"Background job failed: {e}")

def run_assets_job(asset_ids: List[str]):
    logger.info(f"Starting background job for {len(asset_ids)} assets")
    proc = get_processor()
    try:
        assets = proc.get_asset_file_keys(asset_ids)
        if assets:
            proc.process_assets_parallel(assets)
        else:
            logger.info("No assets found for provided IDs")
    except Exception as e:
        logger.error(f"Background job failed: {e}")

@app.post("/process/source/{source_name}")
async def process_source(source_name: str, background_tasks: BackgroundTasks, retry_errors: bool = False):
    if source_name not in ['artlist', 'motionarray']:
        raise HTTPException(status_code=400, detail="Invalid source. Must be 'artlist' or 'motionarray'")
    
    background_tasks.add_task(run_processing_job, source_name, retry_errors)
    return {"message": f"Processing started for {source_name}", "status": "accepted"}

@app.post("/process/assets")
async def process_assets(request: ProcessAssetsRequest, background_tasks: BackgroundTasks):
    if not request.asset_ids:
        raise HTTPException(status_code=400, detail="No asset IDs provided")
    
    background_tasks.add_task(run_assets_job, request.asset_ids)
    return {"message": f"Processing started for {len(request.asset_ids)} assets", "status": "accepted"}

import detect_duplicates

def run_duplicate_detection_job(write_to_snowflake: bool):
    logger.info("Starting duplicate detection job")
    try:
        # Run with skip_confirmation=True since this is an automated job
        detect_duplicates.main(write_to_snowflake_flag=write_to_snowflake, skip_confirmation=True)
        logger.info("Duplicate detection job completed")
    except Exception as e:
        logger.error(f"Duplicate detection job failed: {e}")

@app.post("/detect-duplicates")
async def trigger_duplicate_detection(background_tasks: BackgroundTasks, write_to_snowflake: bool = False):
    background_tasks.add_task(run_duplicate_detection_job, write_to_snowflake)
    return {"message": "Duplicate detection job started", "status": "accepted"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
