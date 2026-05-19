from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from dotenv import load_dotenv
import os

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCS_IP_LOCATION_DIR = "bronze/ip_location"
PROJECT_ID = os.getenv("PROJECT_ID")
BRONZE_DATASET_ID = os.getenv("BRONZE_DATASET_ID")
BRONZE_SUMMARY_TABLE_ID = os.getenv("BRONZE_SUMMARY_TABLE_ID")
BRONZE_PRODUCT_TABLE_ID = os.getenv("BRONZE_PRODUCT_TABLE_ID")
BRONZE_IP_LOCATION_TABLE_ID = os.getenv("BRONZE_IP_LOCATION_TABLE_ID")

def import_csv_from_gcs_to_bigquery():

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
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
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

def create_ip_location_table_with_schema():
    # Khởi tạo BigQuery Client (Tự động ăn theo Google Application Credentials trên VM)
    client = bigquery.Client(project=PROJECT_ID)
    
    # Đường dẫn định danh đầy đủ của bảng trong BigQuery
    table_ref = f"{PROJECT_ID}.{BRONZE_DATASET_ID}.{BRONZE_IP_LOCATION_TABLE_ID}"
    
    # =====================================================================
    # 2. ĐỊNH NGHĨA KHUÔN MẪU SCHEMA (SCHEMA DEFINITION)
    # =====================================================================
    schema = [
        # Các trường phẳng thông thường (Flat Fields)
        bigquery.SchemaField("_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("product_name", "STRING", mode="NULLABLE"),
        
        # Trường giá tiền (Price) - Bắt buộc dùng NUMERIC để đảm bảo độ chính xác tài chính
        bigquery.SchemaField("price", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("discount", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("is_active", "BOOLEAN", mode="NULLABLE"),
        
        # TRƯỜNG HỢP 1: Trường 'option' đa hình (Sau khi đã được Python đồng nhất về mảng)
        # Type là RECORD (Object) và Mode là REPEATED (Mảng) -> Array of Objects
        bigquery.SchemaField(
            "option", 
            "RECORD", 
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("value", "STRING", mode="NULLABLE")
            ]
        ),
        
        # TRƯỜNG HỢP 2: Trường 'cart_products' (Mảng các sản phẩm trong giỏ hàng)
        bigquery.SchemaField(
            "cart_products", 
            "RECORD", 
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("sku", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("qty", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("price", "NUMERIC", mode="NULLABLE") 
            ]
        )
    ]
    
    # =====================================================================
    # 3. TIẾN TRÌNH KHỞI TẠO BẢNG TRÊN CLOUD
    # =====================================================================
    try:
        # Kiểm tra xem bảng đã tồn tại hay chưa
        client.get_table(table_ref)
        print(f"[-] Bảng {table_ref} đã tồn tại từ trước. Không cần tạo mới.")
        
    except NotFound:
        # Nếu chưa có bảng -> Khởi tạo cấu hình bảng mới với Schema đã định nghĩa
        table = bigquery.Table(table_ref, schema=schema)
        
        # Có thể thêm cấu trúc phân mảnh theo thời gian (Partition) nếu cần tối ưu chi phí sau này
        # table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
        
        # Gọi API gửi lệnh tạo bảng lên Google Cloud
        created_table = client.create_table(table)
        print(f"[SUCCESS] Đã khởi tạo bảng thành công: {created_table.full_table_id}")
        print("Schema đã được áp dụng nghiêm ngặt cho bảng này.")
        
    except Exception as e:
        print(f"[CRITICAL ERROR] Quá trình tạo bảng thất bại: {e}")

if __name__ == "__main__":
    import_csv_from_gcs_to_bigquery()