from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from dotenv import load_dotenv
import os

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_IP_LOCATION_DIR = "bronze/ip_location"
GCS_PRODUCT_DIR = "bronze/product_jsonl"
# GCS_SUMMARY_DIR = "bronze/summary_test"
GCS_SUMMARY_DIR = "bronze/summary_raw"

PROJECT_ID = os.getenv("PROJECT_ID")
BRONZE_DATASET_ID = os.getenv("BRONZE_DATASET_ID")
BRONZE_SUMMARY_TABLE_ID = os.getenv("BRONZE_SUMMARY_TABLE_ID")
BRONZE_PRODUCT_TABLE_ID = os.getenv("BRONZE_PRODUCT_TABLE_ID")
BRONZE_IP_LOCATION_TABLE_ID = os.getenv("BRONZE_IP_LOCATION_TABLE_ID")

def create_ip_location_table_from_gcs():

    GCS_URI = f"gs://{GCS_BUCKET_NAME}/{GCS_IP_LOCATION_DIR}/ip_locations.csv"

    # Khởi tạo BigQuery Client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Định danh đường dẫn đầy đủ của bảng đích
    table_ref = f"{PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_IP_LOCATION_TABLE_ID}"
    
    # =====================================================================
    # 2. CẤU HÌNH TIẾN TRÌNH NẠP (LOAD JOB CONFIG)
    # =====================================================================
    job_config = bigquery.LoadJobConfig()
    
    # Chỉ định rõ nguồn vào là file CSV
    job_config.source_format = bigquery.SourceFormat.CSV
    
    # Bỏ qua 1 dòng đầu tiên (Dòng tiêu đề/Header của file CSV)
    job_config.skip_leading_rows = 1
    
    # Ép cấu hình UTF-8 để bảo vệ toàn vẹn chữ tiếng Việt có dấu
    job_config.encoding = "UTF-8"
    
    # Hành động ghi: WRITE_APPEND (Ghi nối tiếp) hoặc WRITE_TRUNCATE (Ghi đè/Làm sạch bảng cũ)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    # =====================================================================
    # 2. TỰ CẤU HÌNH SCHEMA THỦ CÔNG (MANUAL SCHEMA DEFINITION)
    # =====================================================================
    # Bạn định nghĩa các trường theo cấu trúc phẳng của file CSV.
    # Các kiểu dữ liệu phổ biến: STRING, INTEGER, FLOAT64, NUMERIC, BOOLEAN, TIMESTAMP
    job_config.schema = [
        bigquery.SchemaField("ip", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("country_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE")
    ]
    
    # Tắt tính năng tự động nhận diện vì ta đã cung cấp schema tường minh
    job_config.autodetect = False

    # =====================================================================
    # 3. KÍCH HOẠT TIẾN TRÌNH LOAD JOB
    # =====================================================================
    print(f"[*] Đang nạp CSV từ GCS với Schema thủ công nghiêm ngặt...")
    try:
        load_job = client.load_table_from_uri(
            GCS_URI,
            table_ref,
            job_config=job_config
        )
        
        load_job.result() # Chờ tiến trình Cloud hoàn thành
        
        destination_table = client.get_table(table_ref)
        print(f"[SUCCESS] Bảng `{BRONZE_IP_LOCATION_TABLE_ID}` đã áp dụng Schema thành công!")
        print(f"Số lượng dòng hiện tại: {destination_table.num_rows}")
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Quá trình nạp dữ liệu thất bại: {e}")

def create_product_table_from_gcs():
    GCS_FOLDER_URI = f"gs://{GCS_BUCKET_NAME}/{GCS_PRODUCT_DIR}/*.jsonl"

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_PRODUCT_TABLE_ID}"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.encoding = "UTF-8"
    
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = False

    job_config.schema = [
        bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("sku", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("attribute_set_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("attribute_set", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("type_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("min_price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("max_price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("min_price_format", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("max_price_format", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gold_weight", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("none_metal_weight", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("fixed_silver_weight", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("material_design", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("qty", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_type_value", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("category_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("store_code", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("platinum_palladium_info_in_alloy", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("bracelet_without_chain", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("show_popup_quantity_eternity", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE")
    ]
    
    print(f"[*] Đang yêu cầu BigQuery nạp toàn bộ file JSONL từ folder: {GCS_FOLDER_URI}...")
    try:
        load_job = client.load_table_from_uri(
            GCS_FOLDER_URI,
            table_ref,
            job_config=job_config
        )
        
        load_job.result() # Chờ tiến trình Cloud hoàn thành
        
        destination_table = client.get_table(table_ref)
        print(f"[SUCCESS] Bảng `{BRONZE_PRODUCT_TABLE_ID}` đã áp dụng Schema thành công!")
        print(f"Số lượng dòng hiện tại: {destination_table.num_rows}")
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Quá trình nạp dữ liệu thất bại: {e}")

def create_summary_table_from_gcs():
    GCS_FOLDER_URI = f"gs://{GCS_BUCKET_NAME}/{GCS_SUMMARY_DIR}/*.jsonl"

    client = bigquery.Client(project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_SUMMARY_TABLE_ID}"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.autodetect = False

    job_config.schema = [
        bigquery.SchemaField("_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("time_stamp", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ip", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_agent", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("resolution", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("user_id_db", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("device_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("api_version", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("store_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("local_time", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("show_recommendation", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("referrer_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email_address", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("utm_source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("utm_medium", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collection", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("key_search", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("viewing_product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_product_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_clicked_position", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("recommendation_product_position", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_paypal", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField(
            "option",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("alloy", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("diamond", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("shapediamond", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("stone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("pearlcolor", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("finish", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Kollektion", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("kollektion_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quality_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quality", "STRING", mode="NULLABLE")
            ]
        ),
        bigquery.SchemaField(
            "option_object",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("alloy", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("diamond", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("shapediamond", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("stone", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("pearlcolor", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("finish", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("category id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("Kollektion", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("kollektion_id", "STRING", mode="NULLABLE")
            ]
        ),
        bigquery.SchemaField(
            "option_list",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quality_label", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("quality", "STRING", mode="NULLABLE")
            ]
        ),
        bigquery.SchemaField("cat_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("collect_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("order_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "cart_products",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("product_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("amount", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "option",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("option_label", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("option_id", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("value_label", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("value_id", "STRING", mode="NULLABLE")
                    ]
                )
            ]
        )
    ]

    print(f"[*] Đang yêu cầu BigQuery nạp toàn bộ file JSONL từ folder: {GCS_FOLDER_URI}...")
    try:
        load_job = client.load_table_from_uri(
            GCS_FOLDER_URI,
            table_ref,
            job_config=job_config
        )

        load_job.result()

        destination_table = client.get_table(table_ref)
        print(f"[SUCCESS] Bảng `{BRONZE_SUMMARY_TABLE_ID}` đã áp dụng Schema thành công!")
        print(f"Số lượng dòng hiện tại: {destination_table.num_rows}")
    except Exception as e:
        print(f"[CRITICAL ERROR] Quá trình nạp dữ liệu thất bại: {e}")

if __name__ == "__main__":
    # 1. Tạo bảng ip_locations trên BigQuery từ file CSV trong GCS
    create_ip_location_table_from_gcs()

    # 2. Tạo bảng products trên BigQuery từ nhiều file JSONL trong GCS
    create_product_table_from_gcs()

    # 3. Tạo bảng summary_raw trên BigQuery từ nhiều file JSONL trong GCS
    create_summary_table_from_gcs()