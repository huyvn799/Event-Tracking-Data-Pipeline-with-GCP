import functions_framework
from google.cloud import bigquery

# Cấu hình cố định hệ thống của Huy
PROJECT_ID = "gcp-data-glamira-dec-k23"
BRONZE_DATASET_ID = "glamira_bronze"
BRONZE_SUMMARY_TABLE_ID = "summary_raw"
BRONZE_PRODUCT_TABLE_ID = "product_test_trigger"
BRONZE_IP_LOCATION_TABLE_ID = "ip_locations"

@functions_framework.cloud_event
def products_from_gcs_trigger(cloud_event):
    """
    Hàm tự động kích hoạt khi có file JSONL mới được upload lên GCS bucket.
    """
    # 1. Trích xuất thông tin file từ Event của GCS
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    # Chỉ xử lý nếu file thuộc thư mục chỉ định và có định dạng .jsonl
    # Ví dụ: folder 'products_jsonl/' hoặc 'summary_test/'
    if not file_name.endswith(".jsonl"):
        print(f"[-] Bỏ qua file không phải định dạng JSONL: {file_name}")
        return

    gcs_uri = f"gs://{bucket_name}/{file_name}"
    print(f"[*] Phát hiện file mới: {gcs_uri}. Tiến hành nạp vào BigQuery...")

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
    
    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        print(f"[+] Đã gửi thành công Load Job ID: {load_job.job_id} lên BigQuery.")
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Khởi tạo Load Job thất bại cho file {file_name}: {e}")

@functions_framework.cloud_event
def raw_summary_from_gcs_trigger(cloud_event):
    """
    Hàm tự động kích hoạt khi có file JSONL mới được upload lên GCS bucket.
    """
    # 1. Trích xuất thông tin file từ Event của GCS
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    # Chỉ xử lý nếu file thuộc thư mục chỉ định và có định dạng .jsonl
    # Ví dụ: folder 'products_jsonl/' hoặc 'summary_test/'
    if not file_name.endswith(".jsonl"):
        print(f"[-] Bỏ qua file không phải định dạng JSONL: {file_name}")
        return

    gcs_uri = f"gs://{bucket_name}/{file_name}"
    print(f"[*] Phát hiện file mới: {gcs_uri}. Tiến hành nạp vào BigQuery...")

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_SUMMARY_TABLE_ID}"

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.encoding = "UTF-8"
    
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

    try:
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_ref,
            job_config=job_config
        )
        
        print(f"[+] Đã gửi thành công Load Job ID: {load_job.job_id} lên BigQuery.")
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Khởi tạo Load Job thất bại cho file {file_name}: {e}")