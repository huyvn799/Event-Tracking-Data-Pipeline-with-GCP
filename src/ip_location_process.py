import os
import time
import csv
from dotenv import load_dotenv
import pymongo
from IP2Location import IP2Location
from urllib.parse import quote_plus

load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD")) or ""
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# Cấu hình batch
BATCH_SIZE = 50000
CSV_PATH = "output/ip_locations.csv"
UNIQUE_IPS_CSV_PATH = "output/unique_ips.csv"
BIN_PATH = "input/IP-COUNTRY-REGION-CITY.BIN"

os.makedirs("output", exist_ok=True)

def process_ip_locations():
    # Bắt đầu đo thời gian chương trình
    program_start = time.perf_counter()
    
    # Nếu có username, sử dụng authentication, ngược lại kết nối không auth
    if USERNAME:
        connection_string = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
    else:
        connection_string = f"mongodb://{HOST}:{PORT}/"
    
    # Kết nối MongoDB
    try:
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        ip_collection = db["ip_locations"]
    except Exception as e:
        print(f"✗ Lỗi kết nối MongoDB: {e}")
        return

    # Lấy unique IPs sử dụng aggregation với $group để tối ưu cho large dataset
    print("=" * 60)
    print("BẮT ĐẦU LẤY UNIQUE IPs BẰNG AGGREGATION")
    print("=" * 60)
    
    pipeline = [
        {"$group": {"_id": "$ip"}}  # Group theo field "ip" để lấy unique
    ]
    
    get_unique_start = time.perf_counter()
    try:
        unique_ips_cursor = collection.aggregate(
            pipeline,
            allowDiskUse=True,  # Cho phép sử dụng disk nếu cần
            batchSize=50000     # Lấy 50000 documents mỗi batch từ Mongo
        )
        unique_ips = [doc["_id"] for doc in unique_ips_cursor if doc["_id"]]
        get_unique_end = time.perf_counter()
        get_unique_time = get_unique_end - get_unique_start
        
        print(f"✓ Tổng số unique IPs: {len(unique_ips)}")
        print(f"✓ Thời gian lấy unique IPs: {get_unique_time:.2f}s")
    except Exception as e:
        print(f"✗ Lỗi khi lấy unique IPs: {e}")
        client.close()
        return
    
    # Ghi unique IPs vào file CSV
    print("\n" + "=" * 60)
    print("GHI UNIQUE IPs VÀO FILE CSV")
    print("=" * 60)
    
    write_unique_start = time.perf_counter()
    try:
        if os.path.exists(UNIQUE_IPS_CSV_PATH):
            os.remove(UNIQUE_IPS_CSV_PATH)
        
        with open(UNIQUE_IPS_CSV_PATH, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["IP"])  # Header
            for ip in unique_ips:
                writer.writerow([ip])
        
        write_unique_end = time.perf_counter()
        write_unique_time = write_unique_end - write_unique_start
        print(f"✓ Đã ghi {len(unique_ips)} unique IPs vào: {UNIQUE_IPS_CSV_PATH}")
        print(f"✓ Thời gian ghi unique IPs: {write_unique_time:.2f}s\n")
    except Exception as e:
        print(f"✗ Lỗi ghi unique IPs: {e}")
    
     # Load file .BIN sử dụng IP2Location
    print("\n" + "=" * 60)
    print("BẮT ĐẦU MAPPING VỚI FILE BIN")
    print("=" * 60)
    
    try:
        ip2loc = IP2Location(BIN_PATH)
        print(f"✓ Đã load file BIN: {BIN_PATH}\n")
    except Exception as e:
        print(f"✗ Lỗi load file BIN: {e}")
        client.close()
        return
    
    # Khởi tạo thống kê
    total_success = 0
    total_failure = 0
    total_batches = 0
    batch_results = []
    
    # Xử lý theo batch
    for i in range(0, len(unique_ips), BATCH_SIZE):
        batch_count = i // BATCH_SIZE + 1
        total_batches += 1
        batch_ids = unique_ips[i : i + BATCH_SIZE]
        batch_size_actual = len(batch_ids)
        
        batch_start = time.perf_counter()
        batch_success = 0
        batch_failure = 0
        batch_data = []
        
        # Mapping từng IP trong batch
        for ip in batch_ids:
            try:
                rec = ip2loc.get_all(ip)
                country = rec.country_long or "Unknown"
                region = rec.region or "Unknown"
                city = rec.city or "Unknown"
                
                batch_data.append({
                    "IP": ip,
                    "Country": country,
                    "Region": region,
                    "City": city,
                    "Mapped": True
                })
                batch_success += 1
            except Exception:
                batch_data.append({
                    "IP": ip,
                    "Country": "Unknown",
                    "Region": "Unknown",
                    "City": "Unknown",
                    "Mapped": False
                })
                batch_failure += 1
        
        # Ghi batch vào CSV (append mode)
        try:
            # Xóa file CSV cũ nếu tồn tại
            if os.path.exists(CSV_PATH):
                os.remove(CSV_PATH)

            with open(CSV_PATH, 'a', newline='', encoding='utf-8') as csvfile:
                if i == 0:  # Viết header cho lần đầu
                    writer = csv.DictWriter(csvfile, fieldnames=["IP", "Country", "Region", "City", "Mapped"])
                    writer.writeheader()
                else:
                    writer = csv.DictWriter(csvfile, fieldnames=["IP", "Country", "Region", "City", "Mapped"])
                writer.writerows(batch_data)
        except Exception as e:
            print(f"✗ Lỗi ghi CSV batch {batch_count}: {e}")
        
        # Insert batch vào MongoDB
        try:
            if batch_data:
                ip_collection.insert_many(batch_data)
        except Exception as e:
            print(f"✗ Lỗi insert MongoDB batch {batch_count}: {e}")
        
        batch_end = time.perf_counter()
        batch_elapsed = batch_end - batch_start
        
        total_success += batch_success
        total_failure += batch_failure
        
        # In thống kê batch
        print(f"Batch {batch_count:4d}: {batch_size_actual:6d} IPs | "
              f"✓ {batch_success:6d} | ✗ {batch_failure:6d} | "
              f"Thời gian: {batch_elapsed:8.2f}s")
        
        batch_results.append({
            "batch": batch_count,
            "processed": batch_size_actual,
            "success": batch_success,
            "failure": batch_failure,
            "time": batch_elapsed
        })
    
    program_end = time.perf_counter()
    total_time = program_end - program_start
    
    # In thống kê tổng kết
    print("\n" + "=" * 60)
    print("THỐNG KÊ TỔNG KẾT")
    print("=" * 60)
    print(f"Tổng thời gian chạy:        {total_time:.2f}s")
    print(f"Tổng số batch:              {total_batches}")
    print(f"Tổng unique IPs:            {len(unique_ips)}")
    print(f"Tổng mapping thành công:    {total_success}")
    print(f"Tổng mapping thất bại:      {total_failure}")
    print(f"Tỷ lệ thành công:           {(total_success/len(unique_ips)*100):.2f}%")
    print(f"Tỷ lệ thất bại:             {(total_failure/len(unique_ips)*100):.2f}%")
    print(f"\nFile unique IPs:            {UNIQUE_IPS_CSV_PATH}")
    print(f"File CSV mapping:            {CSV_PATH}")
    print(f"Collection MongoDB:         ip_locations")
    print("=" * 60)
    
    # Đóng kết nối
    client.close()