from curl_cffi import requests
import json
import csv
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
from dotenv import load_dotenv
from urllib.parse import quote_plus
import pymongo
import shutil

# --- CẤU HÌNH ---
load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

# --- CẤU HÌNH ---
MAX_WORKERS = 5
MAX_RETRIES = 5
BATCH_SIZE = 50
OUTPUT_DIR = "output/crawl_data4"
LOG_DIR = "output/logs4"
RECORDS_PER_FILE = 1000
CHECKPOINT_BATCH_SIZE = 100
CHECKPOINT_FILE = "checkpoint.json"
SUCCESS_CSV = "success.csv"
FAILED_RETRY_CSV = "failed_retry.csv"
FAILED_FINAL_CSV = "failed_final.csv"

# Danh sách các thông tin bạn THỰC SỰ muốn lấy
KEYS_TO_KEEP = [
    "product_id",
    "name",
    "sku",
    "attribute_set_id",
    "attribute_set",
    "type_id",
    "price",
    "min_price",
    "max_price",
    "min_price_format",
    "max_price_format",
    "gold_weight",
    "none_metal_weight",
    "fixed_silver_weight",
    "material_design",
    "qty",
    "collection",
    "collection_id",
    "product_type",
    "product_type_value",
    "category",
    "category_name",
    "store_code",
    "platinum_palladium_info_in_alloy",
    "bracelet_without_chain",
    "show_popup_quantity_eternity",
    "gender"
]

# Khóa để bảo vệ file và biến đếm
file_lock = threading.Lock()
stats = {'total_success': 0}
ua = UserAgent()

# --- HÀM TRÍCH XUẤT ---
def extract_whitelist(data, keys_to_keep):
    """
    Chỉ giữ lại các key nằm trong danh sách cho trước và loại bỏ phần còn lại.
    """
    return {
        key: value for key, value in data.items() if key in keys_to_keep
    }

def extract_react_data(html_content):
    """
    Hàm dùng chung để trích xuất react_data từ bất kỳ nguồn HTML nào.
    """
    try:
        soup = BeautifulSoup(html_content, 'lxml')
        # Tìm thẻ script có chứa chuỗi "react_data"
        script_tag = soup.find('script', string=re.compile(r'react_data\s*[:=]'))
        
        if not script_tag or not script_tag.string:
            return "MISSING_SCRIPT_TAG"

        # Dùng Regex để cắt lấy phần JSON
        # Pattern này tìm từ dấu { sau react_data cho đến khi gặp dấu }; ở cuối
        match = re.search(r'react_data\s*[:=]\s*({.+?});', script_tag.string, re.DOTALL)
        
        if match:
            json_str = match.group(1)
            data = json.loads(json_str)
            return data
        
        return "MISSING_JSON_MATCH"
    except json.JSONDecodeError:
        return "JSON_DECODE_ERROR"
    except Exception as e:
        return f"EXTRACTION_ERROR_{type(e).__name__}"

# --- GHI FILE AN TOÀN ---
def get_checkpoint():
    with file_lock:
        file_path = os.path.join(LOG_DIR, CHECKPOINT_FILE)
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    return {"last_index": 0, "current_file": "data_part_0.jsonl", "data_file_index": 0}

def save_checkpoint(index, filename, data_file_index):
    with file_lock:
        file_path = os.path.join(LOG_DIR, CHECKPOINT_FILE)
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dump({
                'last_index': index,
                'current_file': filename,
                'data_file_index': data_file_index
            }, ensure_ascii=False))
    return

def safe_write_jsonl(data, stats=stats):
    with file_lock:
        file_index = stats['total_success'] // RECORDS_PER_FILE
        file_path = os.path.join(OUTPUT_DIR, f"data_part_{file_index}.jsonl")
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        stats['total_success'] += 1

def safe_write_csv(file_name, row):
    file_path = os.path.join(LOG_DIR, file_name)
    file_exists = os.path.isfile(file_path)
    with file_lock:
        with open(file_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

def get_finished_ids_from_csv():
    """Đọc file CSV để lấy tập hợp các ID đã thành công (lọc trùng)."""
    finished_ids = set()
    file_path = os.path.join(LOG_DIR, SUCCESS_CSV)
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row: finished_ids.add(str(row[0])) # ID nằm ở cột đầu tiên
    return finished_ids

def get_failed_ids_from_csv():
    """Đọc file CSV để lấy tập hợp các ID đã thất bại (bố sung)."""
    failed_ids = set()
    file_path = os.path.join(LOG_DIR, FAILED_FINAL_CSV)
    if os.path.exists(file_path):
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row: failed_ids.add(str(row[0])) # ID nằm ở cột đầu tiên
    return failed_ids

# --- WORKER CHÍNH ---
def crawl_task(pid):
    # print(f"[DEBUG] Starting PID: {pid}") # Log này cực kỳ quan trọng

    url = f"https://www.glamira.vn/catalog/product/view/id/{pid}"
    try:
        # Giả lập trình duyệt để tránh 403
        headers = {
            # 'User-Agent': ua.random, # Nếu dùng curl_cffi, tham số impersonate sẽ tự động set User-Agent, nên có thể bỏ qua dòng này
            'Referer': 'https://www.glamira.vn/',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        resp = requests.get(
            url,
            headers=headers,
            impersonate="chrome120",
            timeout=15,
            verify=False) # curl_cffi requests
        # resp = requests.get(url, headers=headers, allow_redirects=True, timeout=10) # requests thường
        
        if resp.status_code == 200:
            html = resp.text
            data = extract_react_data(html)
            cleaned_data = extract_whitelist(data, KEYS_TO_KEEP) if isinstance(data, dict) else data
            
            if isinstance(cleaned_data, dict):
                record = {"product_id": pid, "data": cleaned_data, "crawled_at": datetime.now().isoformat()}
                return "SUCCESS", record, pid

            return "ERR_NO_DATA", None, pid
        return f"HTTP_{resp.status_code}", None, pid
    except Exception as e:
        return f"ERR_{type(e).__name__}", None, pid

def get_product_ids_from_db():
    """Hàm này có thể được sử dụng để lấy danh sách product_id trực tiếp từ MongoDB nếu cần."""
    try:
        if USERNAME:
            uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
        else:
            uri = f"mongodb://{HOST}:{PORT}/"
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
        products = list(db["cleaned_product_list"].find({}, {"_id": 0, "product_id": 1}))
        return [str(p['product_id']) for p in products]
    except Exception as e:
        print(f"✗ Lỗi khi kết nối hoặc truy vấn MongoDB: {e}")
        return []

# --- PIPELINE ---
def run_pipeline(product_ids):

    sorted_products_ids = sorted(product_ids)
    current_targets = set()

    checkpoint = get_checkpoint()
    start_index = 0
    current_jsonl_file = f"data_part_0.jsonl"
    data_file_index = 0

    # Nếu có checkpoint, khởi động lại từ đó, đồng thời lọc ra những ID đã thành công để tránh crawl lại
    if checkpoint["last_index"] > 0:
        start_index = checkpoint["last_index"]
        current_jsonl_file = checkpoint["current_file"]
        data_file_index = checkpoint["data_file_index"]
        print(f">>> Phát hiện checkpoint tại index {checkpoint['last_index']}, file {checkpoint['current_file']}.")
        
        # Lấy danh sách ID đã thành công để loại trừ khỏi danh sách crawl lần này
        finished_ids = get_finished_ids_from_csv()
        current_targets = set(str(pid) for pid in sorted_products_ids[start_index:] if str(pid) not in finished_ids)

        # Bổ sung những ID đã thất bại ở lần trước vào danh sách crawl lần này để retry
        failed_ids = get_failed_ids_from_csv()
        current_targets.update(str(pid) for pid in failed_ids if str(pid) not in finished_ids)

        current_targets = set(sorted(current_targets))  # Sắp xếp lại để có thứ tự nhất định

        # Update lại stats['total_success'] dựa trên số lượng ID đã thành công để đảm bảo data_file_index được tính đúng
        stats['total_success'] = len(finished_ids)
        print(f"[*] Resume từ index {start_index}. Cần xử lý: {len(current_targets)} sản phẩm.")
    else:
        current_targets = set(sorted_products_ids)
        print(">>> Không tìm thấy checkpoint, bắt đầu từ đầu.")

    global_start = time.time()
    final_failed_products = []

    for attempt in range(1, MAX_RETRIES + 1):
        attempt_start = time.time()
        win_in_attempt = 0
        fail_in_attempt = 0
        next_retry_list = []
        
        print(f"\n>>> BẮT ĐẦU LẦN {attempt} ({len(current_targets)} SP)")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Ánh xạ Future với Product ID (Mapping Pattern) để tránh lỗi logic nhầm ID
            future_to_pid = {executor.submit(crawl_task, pid): str(pid) for pid in current_targets}
            try:
                for future in as_completed(future_to_pid, timeout=30):
                    pid = future_to_pid[future] # Biết ngay PID kể cả khi future.result() bị lỗi
                    # print(f"✓ Đang chờ kết quả PID {pid}...") # Log này cực kỳ quan trọng để theo dõi tiến trình
                    try:
                        status, task_result, prod_id = future.result(timeout=30) # Thêm timeout cho từng future để tránh treo lâu
                        
                        if status == "SUCCESS":
                            win_in_attempt += 1

                            if pid in next_retry_list:
                                next_retry_list.remove(pid)  # Đảm bảo không có trong danh sách retry

                            safe_write_jsonl(task_result, stats)
                            safe_write_csv(SUCCESS_CSV, {"id": pid, "attempt": attempt, "time": datetime.now()})

                            data_file_index = stats['total_success'] // RECORDS_PER_FILE
                            current_jsonl_file = f"data_part_{data_file_index}.jsonl"
                        else:
                            fail_in_attempt += 1
                            next_retry_list.append({"product_id": str(pid), "status_code": status})
                            safe_write_csv(FAILED_RETRY_CSV, {"id": str(pid), "attempt": attempt, "status_code": task_result.get("status_code"), "time": datetime.now()})
                        
                        # Thống kê mỗi 50 sản phẩm
                        total_processed = win_in_attempt + fail_in_attempt
                        if total_processed % BATCH_SIZE == 0:
                            print(f"   [Thống kê] Đã xong {total_processed} SP | Thành công: {win_in_attempt} | Thất bại: {fail_in_attempt}")

                        # Cập nhật checkpoint sau mỗi batch (100 sản phẩm) được xử lý
                        if total_processed % CHECKPOINT_BATCH_SIZE == 0:
                            save_checkpoint(min(start_index + total_processed, len(sorted_products_ids)), 
                                            current_jsonl_file, 
                                            data_file_index)

                    except Exception as e:
                        print(f"✗ Không lấy được result của future với pid {pid}: {e}")
                        next_retry_list.append({"product_id": str(pid), "status_code": "Timeout"})
                        safe_write_csv(FAILED_RETRY_CSV, {"id": str(pid), "attempt": attempt, "status_code": "Timeout", "time": datetime.now()})
            except Exception as e:
                print(f"✗ Lỗi khi chờ Future hoàn thành: {e}")
                
        print(f"--- Kết quả lượt {attempt}: Thành công {win_in_attempt}, Thất bại {fail_in_attempt}")
        print(f"--- Thời gian lượt: {time.time() - attempt_start:.2f}s")
        
        
        final_failed_products = [item for item in next_retry_list]  # Cập nhật danh sách thất bại cuối cùng sau mỗi lượt

        current_targets = set(str(item["product_id"]) for item in next_retry_list)
        if not current_targets: break # Dừng nếu không còn sản phẩm lỗi

    # Sau 5 lần, ghi những sản phẩm thực sự thất bại
    if final_failed_products:
        print(f"\n>>> Có {len(final_failed_products)} sản phẩm không thể crawl sau {MAX_RETRIES} lần RETRY:")
        for item in final_failed_products:
            safe_write_csv(FAILED_FINAL_CSV, {"id": item["product_id"], "status_code": item["status_code"], "time": datetime.now()})

    print(f"\nTOTAL TIME: {time.time() - global_start:.2f}s | Final Success: {stats['total_success']} | Final Failed: {len(final_failed_products)}")

if __name__ == "__main__":
    # Xoá dữ liệu cũ khi checkpoint == 0 (nếu bắt đầu từ đầu), đồng thời tạo thư mục nếu chưa có
    if get_checkpoint()["last_index"] == 0:
        shutil.rmtree(OUTPUT_DIR) if os.path.exists(OUTPUT_DIR) else None
        shutil.rmtree(LOG_DIR) if os.path.exists(LOG_DIR) else None
    for d in [OUTPUT_DIR, LOG_DIR]: os.makedirs(d, exist_ok=True)

    product_ids = get_product_ids_from_db()
    run_pipeline(product_ids)