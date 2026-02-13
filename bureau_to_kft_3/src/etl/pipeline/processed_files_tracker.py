import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set
from ....src import logger_btk

logger = logger_btk
class ProcessedFilesTracker:
    """
    Tracks processed files to avoid reprocessing on subsequent runs.
    Maintains a JSON file with the status of each file processed.
    """
    
    def __init__(self, status_file_path: str = "processed_files_status.json"):
        self.status_file_path = Path(status_file_path)
        self.status_data = self._load_status()
        self.logs = []
    
    def _load_status(self) -> Dict:
        """Load existing status file or create new one."""
        if self.status_file_path.exists():
            try:
                with open(self.status_file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load status file: {e}. Creating new one.")
                return {}
        return {}
    
    def _save_status(self):
        """Save current status to file."""
        try:
            with open(self.status_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.status_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Could not save status file: {e}")
    
    def is_processed(self, batch_id: str, file_name: str) -> bool:
        """
        Check if a file has been successfully processed for a given batch.
        
        Args:
            batch_id: Unique identifier for the batch (e.g., S3 path or batch name)
            file_name: Name of the file
            
        Returns:
            True if file has been processed successfully, False otherwise
        """
        return (
            batch_id in self.status_data and 
            file_name in self.status_data[batch_id].get('processed_files', [])
        )
    
    def mark_processed(self, batch_id: str, file_name: str, record_count: int = 0):
        """
        Mark a file as successfully processed.
        
        Args:
            batch_id: Unique identifier for the batch
            file_name: Name of the file
            record_count: Number of records processed (optional)
        """
        if batch_id not in self.status_data:
            self.status_data[batch_id] = {
                'processed_files': [],
                'last_updated': None, 
                'final_status' : None
            }
        
        if file_name not in self.status_data[batch_id]['processed_files']:
            self.status_data[batch_id]['processed_files'].append(file_name)
        
        self.status_data[batch_id]['last_updated'] = datetime.now().isoformat()
        self._save_status()
        logger.info(f"[TRACKER] Marked {file_name} as processed for batch {batch_id}")
        
    def mark_final_status(self,batch_id, status):
        
        self.status_data[batch_id]['final_status'] = status
        self._save_status()
        logger.info(f"Final status updated: {status}")
        
    
    def get_processed_files(self, batch_id: str) -> List[str]:
        """
        Get list of processed files for a batch.
        
        Args:
            batch_id: Unique identifier for the batch
            
        Returns:
            List of processed file names
        """
        if batch_id in self.status_data:
            return self.status_data[batch_id].get('processed_files', [])
        return []
    
    def filter_unprocessed(self, batch_id: str, file_list: List[str]) -> List[str]:
        """
        Filter out already processed files from a list.
        
        Args:
            batch_id: Unique identifier for the batch
            file_list: List of all available files
            
        Returns:
            List of files that haven't been processed yet
        """
        processed = set(self.get_processed_files(batch_id))
        unprocessed = [f for f in file_list if f not in processed]
        
        if processed:
            logger.info(f"[TRACKER] Found {len(processed)} already processed files for batch {batch_id}")
            logger.info(f"[TRACKER] Skipping: {', '.join(list(processed)[:5])}" + 
                       (f"... and {len(processed) - 5} more" if len(processed) > 5 else ""))
            self.logs.append(F"Skipped files: {', '.join(list(processed)[:5])}" + 
                       (f"... and {len(processed) - 5} more" if len(processed) > 5 else ""))
        
        return unprocessed, self.logs
    
    def reset_batch(self, batch_id: str):
        """
        Reset processing status for a batch (use with caution).
        
        Args:
            batch_id: Unique identifier for the batch
        """
        if batch_id in self.status_data:
            del self.status_data[batch_id]
            self._save_status()
            logger.warning(f"[TRACKER] Reset all processed files for batch {batch_id}")
