from pathlib import Path

import yaml
from box import ConfigBox
from pyfiglet import figlet_format

def read_yaml(path: Path):
    with open(path, "r") as j:
        content = yaml.safe_load(j)
    config = ConfigBox(content)
    return config


def S3_loc(batch: str, folder_name: str, object_name: str, bucket_name: str) -> str:
    if batch:
        s = f"s3://{bucket_name}/{object_name}/{batch}/"
    else:
        s = f"s3://{bucket_name}/{object_name}/{folder_name}/"
    return s


def generate_ascii_art(name: str):
    ascii_art = figlet_format(name, font="slant")
    print(ascii_art)
    
    
def lender_data_s3_push(df, batch_no, folder_name, filename):
    import io
    from ..etl.pipeline.storage_connector import StorageConnector
    from ..etl.pipeline import config_btk
    
    s3_client = StorageConnector(config_btk).s3
    
    bucket_name = "lenders-data-exchange"
    object_name = "bureau-to-lender"
    s3_path = f"s3://{bucket_name}/{object_name}/{folder_name}/{batch_no}/"
    
    # Convert Spark dataframe to pandas
    pdf = df.toPandas()
    txt_filename = f"{filename}.txt"
    
    buffer = io.StringIO()
    pdf.to_csv(buffer, sep='|', index=False)
    buffer_content = buffer.getvalue()
    
    # Upload directly to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{object_name}/{folder_name}/{batch_no}/{txt_filename}",
            Body=buffer_content.encode('utf-8')
        )
        print(f"✓ Successfully uploaded {txt_filename} to {s3_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to upload {txt_filename}: {str(e)}")
        return False
