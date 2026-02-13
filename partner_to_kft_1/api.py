from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import List
from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

from src import config_ptk, logger_ptk
from src.utils.utilities import StorageConnector
from src.modules.file_reader import ReadFile
from src.modules.data_cleaning import DataProcessor


app = FastAPI(title="Partner to KFT Ingestion API", version="2.0")

# Initialize
s3 = StorageConnector(config_ptk)
read_file_obj = ReadFile(config_ptk)
processor = DataProcessor(config_ptk)




class FileOperationResponse(BaseModel):
    success: bool
    message: str
    data: dict = {}

class BatchInfo(BaseModel):
    partner_name: str
    batch_no: int
    no_of_records: int
    status: str


class FileInfo(BaseModel):
    filename: str
    no_of_records: int
    status: str


class ProcessResult(BaseModel):
    status: str
    original_count: Optional[int] = None
    cleaned_count: Optional[int] = None
    duplicates_found: Optional[int] = None
    missing_kft_id_count: Optional[int] = None
    batch_no: Optional[int] = None
    message: Optional[str] = None


class ProcessRequest(BaseModel):
    partner_name: str
    batch_no: int


class BulkProcessRequest(BaseModel):
    batches: List[ProcessRequest]


@app.get("/")
async def health_check():
    """API health check"""
    return {"status": "healthy", "service": "Partner to KFT Ingestion"}


@app.post("/files/upload", response_model=FileOperationResponse)
async def upload_files(files: List[UploadFile] = File(...)):
    try:
        results = {"total": len(files), "successful": 0, "failed": 0, "batch_no": None, "files": []}
        s3_keys = []
        
        # Validate & Upload to S3
        for file in files:
            file_bytes = await file.read()
            is_valid, df, error_msg = read_file_obj.validate_and_read(file.filename, file_bytes)
            
            if not is_valid:
                results["failed"] += 1
                results["files"].append({"filename": file.filename, "status": "failed", "error": error_msg})
                continue
            
            try:
                key, _, _ = s3.upload_files(file_bytes, dest_filename=file.filename)
                if key:
                    s3_keys.append(key)
                    results["files"].append({"filename": file.filename, "status": "uploaded", "records": len(df)})
            except Exception as e:
                results["failed"] += 1
                results["files"].append({"filename": file.filename, "status": "failed", "error": str(e)})
        
        # Process files
        if s3_keys:
            batch_no = s3.set_batch_number(config_ptk.location.folder_name, datetime.today())
            results["batch_no"] = batch_no
            read_file_obj.duplicate_logs = []
            
            # Fetch partner_id once for the entire batch
            partner_name = config_ptk.location.folder_name
            try:
                from src.modules import session
                import snowflake.snowpark.functions as F
                partner_record = session.table("embed_db_test.partners_data.partners_master")\
                                        .filter(F.col("partner_name") == partner_name)\
                                        .select("partner_id").collect()
                partner_id = partner_record[0][0] if partner_record else None
            except Exception as e:
                logger_ptk.error(f"Error fetching partner_id: {e}")
                partner_id = None

            seen_phones = set()
            
            for s3_key in s3_keys:
                for filename, body in s3.fetch_files_queue([s3_key]):
                    _, snow_df, error_msg = read_file_obj.read_files(
                        filename, body, batch_no, seen_phones=seen_phones, partner_id=partner_id
                    )
                    
                    for file_info in results["files"]:
                        if file_info["filename"] == filename:
                            if error_msg:
                                file_info["status"] = "failed"
                                file_info["error"] = error_msg
                                results["failed"] += 1
                            elif snow_df:
                                file_info["status"] = "success"
                                file_info["saved_records"] = snow_df.count()
                                results["successful"] += 1
            
        
        message = f"Processed {results['total']} files: {results['successful']} succeeded, {results['failed']} failed"
        return FileOperationResponse(success=results["successful"] > 0, message=message, data=results)
        
    except Exception as e:
        logger_ptk.error(f"Upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/logs/duplicates")
async def get_duplicate_logs():
    return {"count": len(read_file_obj.get_duplicate_logs()), "logs": read_file_obj.get_duplicate_logs()}


@app.get("/logs/errors")
async def get_error_logs():
    return {"count": len(read_file_obj.get_schema_errors()), "errors": read_file_obj.get_schema_errors()}


@app.delete("/logs")
async def clear_logs():
    read_file_obj.schema_errors = []
    read_file_obj.duplicate_logs = []
    return {"message": "Logs cleared"}



@app.get("/batches/pending", response_model=List[BatchInfo])
async def get_pending_batches():
    """
    Get all pending batches from all partners.
    Returns batches with status: 'not processed' or 'processed'
    """
    try:
        batches = processor.get_pending_batches()
        return batches
    except Exception as e:
        logger_ptk.error(f"Error fetching pending batches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/batches/{partner_name}/{batch_no}/files", response_model=List[FileInfo])
async def get_batch_files(partner_name: str, batch_no: int):
    """
    Get all files for a specific batch and partner.
    Shows file-level status and record counts.
    """
    try:
        files = processor.get_files_for_batch(partner_name, batch_no)
        if not files:
            raise HTTPException(status_code=404, detail="No files found for this batch")
        return files
    except HTTPException:
        raise
    except Exception as e:
        logger_ptk.error(f"Error fetching files for batch {batch_no}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/batches/process", response_model=ProcessResult)
async def process_batch(request: ProcessRequest):
    """
    Process a single batch.
    Cleans data and moves from RAW to PROCESSED table.
    """
    try:
        result = processor.process_file(request.partner_name, request.batch_no)
        
        if result.get("status") == "failed":
            raise HTTPException(status_code=400, detail=result.get("message"))
        
        return ProcessResult(**result)
    except HTTPException:
        raise
    except Exception as e:
        logger_ptk.error(f"Error processing batch {request.batch_no}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/batches/process-multiple")
async def process_multiple_batches(request: BulkProcessRequest):
    """
    Process multiple batches in sequence.
    Returns results for each batch.
    """
    try:
        results = []
        
        for batch_req in request.batches:
            result = processor.process_file(batch_req.partner_name, batch_req.batch_no)
            results.append({
                "partner_name": batch_req.partner_name,
                "batch_no": batch_req.batch_no,
                "result": result
            })
        
        successful = sum(1 for r in results if r["result"].get("status") == "success")
        failed = len(results) - successful
        
        return {
            "total": len(results),
            "successful": successful,
            "failed": failed,
            "results": results
        }
    except Exception as e:
        logger_ptk.error(f"Error processing multiple batches: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/errors/cleaning")
async def get_cleaning_errors():
    """Get data cleaning errors"""
    try:
        errors = processor.get_data_cleaning_errors()
        return {
            "count": len(errors),
            "errors": errors
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1226)