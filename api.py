from fastapi import FastAPI, UploadFile, File, HTTPException
from typing import List
from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Optional, Any
from snowflake.snowpark.functions import col
from fastapi.middleware.cors import CORSMiddleware
from box import ConfigBox
import yaml

# PARTNER-TO-KFT
from partner_to_kft_1.src import config_ptk, logger_ptk
from partner_to_kft_1.src.utils.utilities import StorageConnector
from partner_to_kft_1.src.modules.file_reader import ReadFile
from partner_to_kft_1.src.modules.data_cleaning import DataProcessor


# KFT TO BUREAU
from kft_to_bureau_2.src import session_ktb, config_ktb
from kft_to_bureau_2.src.modules.equifax import PartnerData
from kft_to_bureau_2.src.modules.experian import ExperianData
from kft_to_bureau_2.src.utils.utilities import S3Connector

# BUREAU TO KFT
from bureau_to_kft_3.src.etl.pipeline.file_extractor import FileExtractor
from bureau_to_kft_3.src.etl.pipeline.storage_connector import StorageConnector as BureauStorageConnector
from bureau_to_kft_3.src.etl.pipeline.stage_creator import StageCreator
from bureau_to_kft_3.src.etl.pipeline import ApplyTransformations, ApplyTransformations_add_tli
from bureau_to_kft_3.src.utils.exceptions import SchemaValidationError, FileFormatError
from bureau_to_kft_3.src.etl.pipeline import session_btk

# KFT TO LENDER
from kft_to_lender_4.src import config_ktl, logger_ktl
from kft_to_lender_4.src.modules.data_preparation import LendersData
from kft_to_lender_4.src.modules.data_ingestion import IngestData

# LENDER TO KFT
from lender_to_kft_5.src.modules.data_ingestion import IngestData as LendersDataIngest
from lender_to_kft_5.src import config_ltk, logger_ltk

app = FastAPI(title="Embedded Finance API", version="2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PARTNER-TO-KFT
s3 = StorageConnector(config_ptk)
read_file_obj = ReadFile(config_ptk)
processor = DataProcessor(config_ptk)

# KFT TO BUREAU
partner_data_obj = PartnerData(config_ktb, s3)
experian_data_obj = ExperianData(config_ktb, s3)

# KFT TO LENDER
lender_data_module = LendersData(config_ktl)
ingest_data_module = IngestData(config_ktl)

# LENDER TO KFT
lenders_offers =LendersDataIngest(config_ltk)
# PARTNER-TO-KFT
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
    
# KFT TO BUREAU
class BatchProcessRequest(BaseModel):
    batch_numbers: List[str]
    bureau_type: str  # "equifax" or "experian"
    days_threshold: Optional[int] = 90

class ExperianSendRequest(BaseModel):
    scrub_batches: List[str]
    send_type: str  # "all" or "filtered"
    score_threshold: Optional[int] = 600
    
# KFT TO LENDER

class BatchRequest(BaseModel):
    lender_name: str

class ProcessRequest(BaseModel):
    lender_name: str
    lender_display_name: str
    selected_batches: List[str]

class ShortlistRequest(BaseModel):
    new_batch_folder: str
    l_name: str
    b_types: List[str]
    
#  LENDER TO KFT 
class LenderResponse(BaseModel):
    lender_name: str
    batch : str
# ------------- PARTNER-TO-KFT ---------------
@app.get("/")
async def health_check():
    """API health check"""
    return {"status": "healthy", "service": "Partner to KFT Ingestion"}
@app.get("/health")
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
                from partner_to_kft_1.src.modules import session_ptk
                import snowflake.snowpark.functions as F
                partner_record = session_ptk.table("embed_db_test.partners_data.partners_master")\
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


@app.delete("/clear-logs")
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

# ---------------- KFT TO BUREAU ----------------
# data fetching for ui
@app.get("/api/summary/{bureau_type}")
async def get_summary(bureau_type: str):
    """Fetch consolidated summary showing all batches with their processing status"""
    try:
        if bureau_type not in ["equifax", "experian"]:
            raise HTTPException(status_code=400, detail="Invalid bureau type")
        
        if bureau_type == "equifax":
            bureau_data_table = "EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA"
            bureau_resp_table = "EMBED_DB_TEST.EQUIFAX_SCRUB.AR"
            bureau_resp_col = "REFERENCE_NO"
        else:
            bureau_data_table = "EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA"
            bureau_resp_table = "EMBED_DB_TEST.EXPERIAN_SCRUB.AR"
            bureau_resp_col = "CUSTOMER_ID"

        query = f"""
        SELECT 
            ppd.PARTNER_NAME,
            ppd.BATCH_NO,
            MAX(raw.CREATED_AT) as RECEIVED_DATE,
            COUNT(DISTINCT ppd.KFT_ID) as TOTAL_RECORDS,
            COUNT(DISTINCT bd.KFT_ID) as SENT_COUNT,
            COUNT(DISTINCT br.{bureau_resp_col}) as RESPONSE_COUNT,
            MAX(bd.SCRUB_BATCH_NO) as SCRUB_BATCH_NO,
            COUNT(DISTINCT ex.KFT_ID) as SENT_TO_EXPERIAN_COUNT,
            COUNT(DISTINCT exr.CUSTOMER_ID) as EXPERIAN_RESPONSE_COUNT
        FROM EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_PROCESSED_DATA ppd
        JOIN EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_RAW_DATA raw ON ppd.PARTNER_REFERENCE_ID = raw.PARTNER_REFERENCE_ID
        LEFT JOIN {bureau_data_table} bd ON ppd.KFT_ID = bd.KFT_ID
        LEFT JOIN {bureau_resp_table} br ON bd.BUREAU_ID = br.{bureau_resp_col}
        LEFT JOIN EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA ex ON ppd.KFT_ID = ex.KFT_ID
        LEFT JOIN EMBED_DB_TEST.EXPERIAN_SCRUB.AR exr ON ex.BUREAU_ID = exr.CUSTOMER_ID
        GROUP BY ppd.PARTNER_NAME, ppd.BATCH_NO
        ORDER BY MAX(raw.CREATED_AT) DESC
        """
        df = session_ktb.sql(query).to_pandas()
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pending/{bureau_type}")
async def get_pending(bureau_type: str, days_threshold: int = 90):
    """Fetch batches that have pending/unprocessed records"""
    try:
        if bureau_type not in ["equifax", "experian"]:
            raise HTTPException(status_code=400, detail="Invalid bureau type")
        
        if bureau_type == "equifax":
            bureau_data_table = "EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA"
        else:
            bureau_data_table = "EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA"
        
        query = f"""
        SELECT 
            ppd.PARTNER_NAME,
            ppd.BATCH_NO,
            MAX(raw.CREATED_AT) as RECEIVED_DATE,
            COUNT(DISTINCT ppd.KFT_ID) as PENDING_RECORDS,
            MAX(bd.SCRUB_BATCH_NO) as LAST_SCRUB_BATCH
        FROM EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_PROCESSED_DATA ppd
        JOIN EMBED_DB_TEST.PARTNERS_DATA.PARTNERS_RAW_DATA raw ON ppd.PARTNER_REFERENCE_ID = raw.PARTNER_REFERENCE_ID
        LEFT JOIN {bureau_data_table} bd ON ppd.KFT_ID = bd.KFT_ID
        WHERE ppd.SENT_TO_SCRUB = FALSE 
           OR ppd.SCRUB_DATE < DATEADD(day, -{days_threshold}, CURRENT_DATE())
        GROUP BY ppd.PARTNER_NAME, ppd.BATCH_NO
        HAVING COUNT(DISTINCT ppd.KFT_ID) > 0
        ORDER BY MAX(raw.CREATED_AT) DESC
        """
        df = session_ktb.sql(query).to_pandas()
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/experian-tab/{score_threshold}")
async def get_experian_tab_data(score_threshold: int = 600, days_threshold: int = 90):
    """Fetch data for the Experian processing tab (Equifax → Experian flow)"""
    try:
        query = f"""
        SELECT 
            eq.SCRUB_BATCH_NO,
            MAX(eq.SCRUB_DATE) as SCRUB_DATE,
            MAX(ar.CREATED_AT) as EQUIFAX_RESPONSE_DATE,
            COUNT(DISTINCT eq.KFT_ID) as SENT_TO_EQUIFAX,
            COUNT(DISTINCT ar.REFERENCE_NO) as EQUIFAX_HITS,
            COUNT(DISTINCT eq.KFT_ID) - COUNT(DISTINCT ar.REFERENCE_NO) as NO_HITS,
            COUNT(DISTINCT CASE WHEN ar.SCORE >= {score_threshold} THEN eq.KFT_ID END) as SCORE_GTE_INPUT,
            COUNT(DISTINCT ex.KFT_ID) as SENT_TO_EXPERIAN,
            COUNT(DISTINCT exr.CUSTOMER_ID) as EXPERIAN_RESPONSE
        FROM EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EQUIFAX_DATA eq
        LEFT JOIN EMBED_DB_TEST.EQUIFAX_SCRUB.AR ar ON eq.BUREAU_ID = ar.REFERENCE_NO
        LEFT JOIN EMBED_DB_TEST.BUREAU_DATA_EXCHANGE.EXPERIAN_DATA ex ON eq.KFT_ID = ex.KFT_ID
        LEFT JOIN EMBED_DB_TEST.EXPERIAN_SCRUB.AR exr ON ex.BUREAU_ID = exr.CUSTOMER_ID
        WHERE eq.SCRUB_DATE >= DATEADD(day, -{days_threshold}, CURRENT_DATE())
        GROUP BY eq.SCRUB_BATCH_NO
        ORDER BY MAX(eq.SCRUB_DATE) DESC
        """
        df = session_ktb.sql(query).to_pandas()
        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/experian-status/{scrub_batch_no}")
async def get_experian_status(scrub_batch_no: str):
    """Check if Equifax response has been received for a scrub batch"""
    try:
        query = f"""
        SELECT COUNT(*) as record_count
        FROM EMBED_DB_TEST.EQUIFAX_SCRUB.AR
        WHERE BATCH_NO = 'batch_{scrub_batch_no}'
        """
        result = session_ktb.sql(query).collect()
        record_count = result[0]['RECORD_COUNT'] if result else 0
        return {"has_response": record_count > 0, "record_count": record_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


#  data processing 
@app.post("/api/process/equifax")
async def process_equifax_batches(request: BatchProcessRequest):
    """Process selected batches and send to Equifax"""
    try:
        shortlisted_sf = partner_data_obj.shortlist_data(days_threshold=request.days_threshold)
        batches_dict = {}
        total_records = 0
        warnings = []
        
        for batch in request.batch_numbers:
            batch_df = shortlisted_sf.filter(col("BATCH_NO") == batch)
            record_count = batch_df.count()
            
            if record_count > 0:
                batches_dict[batch] = batch_df
                total_records += record_count
            else:
                warnings.append(f"Batch {batch}: No pending records found")
        
        if not batches_dict:
            raise HTTPException(status_code=400, detail="No valid batches to process")
        
        result = partner_data_obj.process_partner_table_batch(batches_dict)
        
        return {
            "success": True,
            "message": f"All {len(batches_dict)} batch(es) combined and sent to Equifax",
            "total_records": total_records,
            "warnings": warnings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/process/experian")
async def process_experian_batches(request: BatchProcessRequest):
    """Process selected batches and send to Experian"""
    try:
        shortlisted_sf = partner_data_obj.shortlist_data(days_threshold=request.days_threshold)
        batches_dict = {}
        total_records = 0
        warnings = []
        
        for batch in request.batch_numbers:
            batch_df = shortlisted_sf.filter(col("BATCH_NO") == batch)
            record_count = batch_df.count()
            
            if record_count > 0:
                batches_dict[batch] = batch_df
                total_records += record_count
            else:
                warnings.append(f"Batch {batch}: No pending records found")
        
        if not batches_dict:
            raise HTTPException(status_code=400, detail="No valid batches to process")
        
        result = experian_data_obj.process_partner_table_batch(batches_dict)
        
        return {
            "success": True,
            "message": f"All {len(batches_dict)} batch(es) combined and sent to Experian",
            "total_records": total_records,
            "warnings": warnings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/send-to-experian")
async def send_to_experian(request: ExperianSendRequest):
    """Send data from Equifax to Experian"""
    try:
        success_batches = []
        failed_batches = []
        
        for batch in request.scrub_batches:
            try:
                if request.send_type == "all":
                    experian_data_obj.send_all_data(batch)
                    success_batches.append({"batch": batch, "type": "all_data"})
                else:  # filtered
                    experian_data_obj.send_shortlisted_data(batch, request.score_threshold)
                    success_batches.append({
                        "batch": batch, 
                        "type": "filtered", 
                        "score_threshold": request.score_threshold
                    })
            except Exception as e:
                failed_batches.append({"batch": batch, "error": str(e)})
        
        return {
            "success": len(failed_batches) == 0,
            "success_count": len(success_batches),
            "failed_count": len(failed_batches),
            "success_batches": success_batches,
            "failed_batches": failed_batches
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))  


# ------------ BUREAU TO KFT -------------
CONFIG_PATH = "bureau_to_kft_3\\config\\config.yaml"
class PipelineRequest(BaseModel):
    bureau: str
    batch: str
    files: Optional[List[str]] = None  
    mode: str = "Automatic" 

def load_config_raw():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def save_config_raw(cfg):
    with open(CONFIG_PATH, "w") as f:
        yaml.safe_dump(cfg, f)

@app.get("/config")
async def get_config():
    try:
        return load_config_raw()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/config")
async def update_config(cfg: Dict[str, Any]):
    try:
        save_config_raw(cfg)
        return {"message": "Config updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/batches/bureau/{bureau_name}")
async def get_bureau_batches(bureau_name: str):
    try:
        temp_config = load_config_raw()
        base_object_name = "bureau-to-kft"
        target_prefix = f"{base_object_name}/{bureau_name}"
        
        temp_config["location"]["object_name"] = target_prefix
        temp_config_box = ConfigBox(temp_config)
        
        connector = BureauStorageConnector(temp_config_box)
        batches = connector.get_available_batches(prefix_override=target_prefix)
        return {"batches": batches, "target_prefix": target_prefix}
    except Exception as e:
        logger_ktl.error(f"Error in get_bureau_batches: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/{bureau_name}/{batch_name}")
async def get_files(bureau_name: str, batch_name: str):
    try:
        temp_config = load_config_raw()
        base_object_name = "bureau-to-kft"
        target_prefix = f"{base_object_name}/{bureau_name}"
        
        temp_config["location"]["object_name"] = target_prefix
        temp_config["location"]["batch"] = batch_name
        temp_config_box = ConfigBox(temp_config)
        
        connector = StorageConnector(temp_config_box)
        files_info, _ = connector.get_list_of_sets_available_in_s3()
        files = files_info[0] if isinstance(files_info, tuple) else files_info
        return {"files": files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run-pipeline")
async def run_pipeline(request: PipelineRequest):
    try:
        # 1. Update Config
        cfg = load_config_raw()
        cfg["name"] = request.bureau
        if 'experian' in request.bureau:
            cfg["schema"] = 'experian_scrub'
        else:
            cfg["schema"] = 'equifax_scrub'
        
        base_object_name = "bureau-to-kft"
        target_prefix = f"{base_object_name}/{request.bureau}"
        cfg["location"]["object_name"] = target_prefix
        cfg["location"]["batch"] = request.batch
        cfg["location"]["identity_tag"] = f"{request.batch}"
        save_config_raw(cfg)
        
        # Reload snowflake context if needed
        # Note: session_btk is imported from src.etl.pipeline which might already be initialized.
        # We might need to switch database/schema dynamically.
        session_btk.sql(f'use database {cfg["database"]}').collect()
        session_btk.sql(f'use schema {cfg["schema"]}').collect()
        
        config_box = ConfigBox(cfg)
        
        # 2. Extract
        connector = StorageConnector(config_box)
        sets_info, s3_url = connector.get_list_of_sets_available_in_s3()
        sets = sets_info[0] if isinstance(sets_info, tuple) else sets_info
        
        if not sets:
             return {"success": False, "message": "No files found in this batch to process."}

        StageCreator(config_box).create_stage('ds')
        
        extractor = FileExtractor(config_box)
        if request.mode == "Manual" and request.files:
            extractor.perform_extraction(available_files=request.files, s3_path=target_prefix)
            processed_count = len(request.files)
        else:
            extractor.perform_extraction(sets_info, s3_url)
            processed_count = len(sets)
            
        return {
            "success": True, 
            "message": f"Pipeline completed successfully. Processed {processed_count} files.",
            "processed_count": processed_count
        }

    except SchemaValidationError as e:
        raise HTTPException(status_code=400, detail=f"Schema mismatch: {str(e)}")
    except FileFormatError as e:
        raise HTTPException(status_code=400, detail=f"Invalid format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    
# -------------- KFT TO LENDER ----------------


@app.get("/api/batches/lender/{lender_name}")
async def get_lender_batches(lender_name: str):
    try:
        bucket_name = config_ktl.location.bucket_name
        # Listing from bureau-to-lender as per request
        prefix = f"bureau-to-lender/{lender_name}/"
        
        s3 = lender_data_module.s3_client
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        except Exception as s3_error:
            logger_ktl.error(f"S3 list_objects_v2 failed: {s3_error}")
            raise HTTPException(status_code=500, detail=f"S3 error: {str(s3_error)}")
        
            
        batches = []
        if 'CommonPrefixes' in response:
            for cp in response['CommonPrefixes']:
                batch_folder = cp['Prefix'][len(prefix):].rstrip('/')
                # Assuming batch folders here also might start with batch_ or just be the folder name
                if batch_folder:
                    batches.append(batch_folder)
        
        return {"batches": batches, "prefix": prefix, "bucket": bucket_name}
          
    except HTTPException:
        raise
    except AttributeError as ae:
        logger_ktl.error(f"Configuration error: {ae}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Configuration error: {str(ae)}")
    except Exception as e:
        logger_ktl.error(f"Unexpected error in get_lender_batches: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.post("/process")
async def process_batches(request: ProcessRequest):
    try:
        bucket_name = config_ktl.location.bucket_name
        # Source from bureau-to-lender
        source_prefix = f"bureau-to-lender/{request.lender_name}/"
        # Target to kft-to-lender
        target_base_prefix = f"kft-to-lender/{request.lender_name}"
        
        new_batch_folder = lender_data_module.process_and_upload_files(
            bucket_name, 
            source_prefix, 
            target_base_prefix,
            request.selected_batches
        )
        
        if not new_batch_folder:
             raise HTTPException(status_code=400, detail="No files were processed.")
             
        return {"new_batch_folder": new_batch_folder}
    except Exception as e:
        logger_ktl.exception(e)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/shortlist")
async def shortlist_data(request: ShortlistRequest):
    try:
        ingest_data_module.process_shortlisting(
            request.new_batch_folder, 
            request.l_name, 
            request.b_types
        )
        return {"status": "success", "message": "Shortlisting and Ingestion completed successfully!"}
    except Exception as e:
        logger_ktl.exception(e)
        raise HTTPException(status_code=500, detail=str(e))
    
#  ----------------- LENDER TO KFT -----------------

@app.get("/api/{lender_name}/fetch-batches")
async def fetch_lender_batches(lender_name: str):
    try:
        
        config_ltk["location"]["lender_name"] = lender_name
        
        # Use config with StorageConnector
        connector = StorageConnector(ConfigBox(config_ltk))
        batches = connector.get_available_batches()
        return {"batches": batches}
    except Exception as e:
        logger_ltk.error(f"Error in fetch_lender_batches: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    

@app.get("/lenders-response")
async def process_lender_response(request : LenderResponse):
    try:
        lenders_offers.process_file(request.lender_name)
        return {'status': "success", "message": "Lenders response processed and saved successfully"}
    except Exception as e:
        logger_ltk.exception(e)
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=1226)