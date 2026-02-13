from datetime import datetime
from collections import defaultdict
from ..modules import session_ktl
from ...src import config_ktl, logger_ktl
from ..utils.utilities import StorageConnector

logger = logger_ktl
class LendersData:
    def __init__(self, config):
        self.config = config
        self.s3_client = StorageConnector(config).s3
        
    def fetch_files(self, bucket_name, prefix, selected_batches=None):
        files = defaultdict(list)
        prefix = prefix.rstrip("/") + "/"
        
       
        response = self.s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )

        if "Contents" not in response:
            return files

        for obj in response["Contents"]:
            key = obj["Key"]

            if key.endswith("/"):
                continue

            relative_path = key[len(prefix):]
            parts = relative_path.split("/")

            if len(parts) < 2:
                continue

            batch_name = parts[0]
            file_name = parts[1]

            if selected_batches is None or batch_name in selected_batches:
                files[batch_name].append(file_name)

        return files

    def fetch_file_content(self, bucket_name, file_key):
        try:
            obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            return obj['Body'].read().decode('utf-8')
        except Exception as e:
            logger.error(f"Error fetching file {file_key}: {e}")
            return ""

    def identify_common_and_unique_files(self, files_in_batches):
        file_name_to_batches = defaultdict(list)
        
        for batch_name, file_names in files_in_batches.items():
            for file_name in file_names:
                file_name_to_batches[file_name].append(batch_name)
        
        common_files = {file_name: batches for file_name, batches in file_name_to_batches.items() if len(batches) > 1}
        unique_files = {file_name: batches[0] for file_name, batches in file_name_to_batches.items() if len(batches) == 1}
        
        return common_files, unique_files

    def process_and_upload_files(self, bucket_name, source_prefix, target_base_prefix, selected_batches):
        logger.info(f"Processing batches: {selected_batches} for {target_base_prefix}")
        
        today_date = datetime.now().strftime("%d-%m-%Y")
        lender_folder = target_base_prefix.split('/')[-1]
        batch_no = StorageConnector(self.config).set_batch_number(lender_folder)
        logger.info(f"Generated new lender_batch_no: {batch_no}")
        
        batch_name = f"lender_batch_{batch_no}"
        target_prefix = f"{target_base_prefix}/{batch_name}_{today_date}"

        files_in_batches = self.fetch_files(bucket_name, source_prefix, selected_batches)
        
        if not files_in_batches:
            logger.warning(f"No files found for selected batches in {source_prefix}")
            return None
        
        common_files, unique_files = self.identify_common_and_unique_files(files_in_batches)
        
        # Process Common Files: Concatenate
        for common_file, batches in common_files.items():
            concatenated_content = ''
            
            for i, b_name in enumerate(batches):
                file_key = f"{source_prefix.rstrip('/')}/{b_name}/{common_file}"
                file_content = self.fetch_file_content(bucket_name, file_key)

                if file_content:
                    lines = file_content.splitlines()
                    if not lines:
                        continue
                    
                    if i == 0:
                        # Keep header for the first file
                        concatenated_content += "\n".join(lines) + "\n"
                    else:
                        # Skip header for subsequent files
                        if len(lines) > 1:
                            concatenated_content += "\n".join(lines[1:]) + "\n"
            
            if concatenated_content:
                target_file_key = f"{target_prefix}/{common_file}"
                self.s3_client.put_object(
                    Bucket=bucket_name, 
                    Key=target_file_key, 
                    Body=concatenated_content.encode('utf-8')
                )
                logger.info(f"Uploaded concatenated file: {target_file_key}")
        
        # Process Unique Files: Copy as is
        for unique_file, b_name in unique_files.items():
            source_file_key = f"{source_prefix.rstrip('/')}/{b_name}/{unique_file}"
            target_file_key = f"{target_prefix}/{unique_file}"
            
            copy_source = {'Bucket': bucket_name, 'Key': source_file_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=bucket_name,
                Key=target_file_key
            )
            logger.info(f"Copied unique file: {target_file_key}")

        # Update logs to include the new batch number for next increment
        self.update_log_file(bucket_name, target_base_prefix, batch_no)
        
        return f"{batch_name}_{today_date}"

    def update_log_file(self, bucket_name, target_base_prefix, batch_no):
        lender_folder = target_base_prefix.split('/')[-1]
        log_file_key = f"kft-to-lender/{lender_folder}/{self.config.location.log_file}"
        
        log_entry = f"[{datetime.now().isoformat()}] lender_batch_no: {batch_no}\n"
        
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=log_file_key)
            current_logs = response['Body'].read().decode('utf-8')
            new_logs = current_logs + log_entry
        except self.s3_client.exceptions.NoSuchKey:
            new_logs = log_entry
            
        self.s3_client.put_object(Bucket=bucket_name, Key=log_file_key, Body=new_logs.encode('utf-8'))
        logger.info(f"Updated log file: {log_file_key}")


    