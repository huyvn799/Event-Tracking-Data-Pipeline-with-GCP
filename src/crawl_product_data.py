import pymongo
import csv
import json
import asyncio
import aiohttp
import aiofiles
import time
import os
import re
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from urllib.parse import quote_plus
from datetime import datetime
import random
from curl_cffi.requests import AsyncSession
from playwright.async_api import async_playwright

# --- CẤU HÌNH ---
load_dotenv()

USERNAME = os.getenv("MONGO_USERNAME") or ""
PASSWORD = quote_plus(os.getenv("MONGO_PASSWORD") or "")
HOST = os.getenv("MONGO_HOST") or "localhost"
PORT = os.getenv("MONGO_PORT") or "27017"
DB_NAME = os.getenv("MONGO_DB_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

EVENT_FILTER = [
    "view_product_detail",
    "select_product_option",
    "select_product_option_quality",
    "add_to_cart_action",
    "product_detail_recommendation_visible",
    "product_detail_recommendation_noticed",
    "product_view_all_recommend_clicked"
]

OUTPUT_DIR = "output"
LOG_DIR = "output/logs2"
CRAWL_DATA_DIR = "output/crawl_data_2"
JSONL_FILE = f"{CRAWL_DATA_DIR}/product_data_{{}}.jsonl"
SUCCESS_CSV = f"{OUTPUT_DIR}/unique_product_ids.csv"
FAILED_CSV = f"{OUTPUT_DIR}/failed_product_ids.csv"
CRAWL_URL_TEMPLATE = "https://www.glamira.vn/catalog/product/view/id/{}"
# GIẢM CONCURRENCY ĐỂ TRÁNH BỊ CHẶN IP NHANH
MAX_CONCURRENT_REQUESTS = 15
# BATCH_SIZE = 100
RATE_LIMIT = 50 # SỐ REQUEST TỐI ĐA / GIÂY

RECORDS_PER_FILE = 1000
MAX_RETRIES = 3
BATCH_SIZE = 50  # Thống kê mỗi 50 sản phẩm
NUM_PW_WORKERS = 4
NUM_AIO_WORKERS = 15
NUM_CURL_WORKERS = 10
for d in [OUTPUT_DIR, LOG_DIR, CRAWL_DATA_DIR]: os.makedirs(d, exist_ok=True)

# Khởi tạo KHÓA để chống Race Condition
file_lock = asyncio.Lock()

# DANH SÁCH USER-AGENTS ĐỂ XOAY VÒNG
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
]

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

KEYS_TO_REMOVE = ["visible_contents",
                    "configure_mode",
                    "included_chain_weight",
                    "quick_options",
                    "associate",
                    "bestseller",
                    "designProvider",
                    "product_price",
                    "discount_custom_options",
                    "options",
                    "compare_sizes",
                    "option_dependent",
                    "media_image",
                    "media_video",
                    "preconfigure",
                    "attributes",
                    "dimension_guide",
                    "attributes_link",
                    "super_data",
                    "quantity_option",
                    "min_price_format",
                    "max_price_format"
]


# --- PHẦN 1: XỬ LÝ ĐỒNG BỘ (SYNC) ---
def prepare_data():
    """Giai đoạn 1: Lọc và chuẩn bị danh sách ID từ MongoDB."""
    print("--- [1/3] GIAI ĐOẠN: TRÍCH XUẤT MONGODB (SYNC) ---")
    start_time = time.perf_counter()
    
    try:
        if USERNAME:
            uri = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/"
        else:
            uri = f"mongodb://{HOST}:{PORT}/"
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=10000)
        db = client[DB_NAME]
    except Exception as e:
        print(f"✗ Lỗi kết nối MongoDB: {e}")
        return
    
    pipeline = [
        {
            "$match": {
                "collection": {
                    "$in": EVENT_FILTER
                },
                "$or": [
                    {"product_id": {"$exists": True, "$ne": None}},
                    {"viewing_product_id": {"$exists": True, "$ne": None}}
                ]
            }
        },
        {
            "$project": {
                "product_id": { "$ifNull": ["$product_id", "$viewing_product_id"] },
                "url": {
                    "$cond": {
                        "if": {"$eq": ["$collection", "product_view_all_recommend_clicked"]},
                        "then": "$referrer_url",
                        "else": "$current_url"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$product_id",
                "first_url": {"$first": "$url"}
            }
        },
        {
            "$project": {
                "product_id": "$_id",
                "first_url": 1,
                "url_request": { "$concat": [CRAWL_URL_TEMPLATE.replace("{}", ""), "$_id"] },
                "_id": 0
            }
        },
        {
            "$out": "cleaned_product_list"
        }
    ]

    try:
        db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True, batchSize=10000)
        products = list(db["cleaned_product_list"].find())
    except Exception as e:
        print(f"✗ Lỗi khi chạy aggregation: {e}")
        client.close()
        return
    
    with open(SUCCESS_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["product_id"])
        for doc in products:
            writer.writerow([doc["product_id"]])
        
    duration = time.perf_counter() - start_time
    print(f"-> Hoàn tất chuẩn bị {len(products)} sản phẩm. Thời gian: {duration:.2f}s\n")
    return products

# --- PHẦN 2: ASYNC CRAWLING ---
def get_random_headers():
    """
    Tạo bộ hồ sơ Header đồng bộ (Consistent Headers) để vượt qua kiểm tra Client Hints.
    """
    browser_profiles = [
        {
            "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "ch_ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "platform": '"Windows"',
            "mobile": "?0"
        },
        {
            "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "ch_ua": '"Not A(Brand";v="99", "Google Chrome";v="120", "Chromium";v="120"',
            "platform": '"macOS"',
            "mobile": "?0"
        },
        {
            "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "ch_ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "platform": '"Linux"',
            "mobile": "?0"
        }
    ]

    profile = random.choice(browser_profiles)
    
    return {
        'User-Agent': profile["ua"],
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': random.choice(['en-US,en;q=0.9', 'vi-VN,vi;q=0.9,en;q=0.8', 'en-GB,en;q=0.9']),
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'authority': 'www.glamira.vn',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'max-age=0',
        'referer': 'https://www.google.com/',
        'sec-ch-ua': profile["ch_ua"],
        'sec-ch-ua-mobile': profile["mobile"],
        'sec-ch-ua-platform': profile["platform"],
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'cross-site',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1'
    }

def filter_json(data, keys_to_remove):
    """
    Hàm đệ quy để xóa các key không cần thiết khỏi Dictionary hoặc List.
    """
    if isinstance(data, dict):
        # Tạo bản sao của các key để tránh lỗi 'dictionary changed size during iteration'
        for key in list(data.keys()):
            if key in keys_to_remove:
                del data[key]
            else:
                filter_json(data[key], keys_to_remove)
    elif isinstance(data, list):
        for item in data:
            filter_json(item, keys_to_remove)
    return data

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

async def fetch_pw(context, pid):
    page = await context.new_page()
    try:
        # Chặn tài nguyên không cần thiết để tăng tốc
        await page.route("**/*.{png,jpg,jpeg,svg,css,woff2}", lambda route: route.abort())
        url = CRAWL_URL_TEMPLATE.format(pid)

        response =await page.goto(url, wait_until="domcontentloaded", timeout=15000)

        status = response.status if response else 0
        if status == 200:
            # Giả sử lấy được react_data ở đây
            content = await page.content()
            react_data = extract_react_data(content)
            whitelisted_data = extract_whitelist(react_data, KEYS_TO_KEEP)
            return "SUCCESS", {"id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat(), "method": "playwright"}
        
        return f"PW_HTTP_{status}", None
    except Exception as e:
        return f"PW_ERR_{type(e).__name__}", None
    finally:
        await page.close()

async def fetch_curl(session, pid):
    """Xử lý với curl cffi đơn lẻ và phân loại lỗi chi tiết."""
    url = CRAWL_URL_TEMPLATE.format(pid)
    try:
        # curl_cffi giúp giả lập trình duyệt thật để tránh 403
        response = await session.get(url, timeout=10, impersonate="chrome110")
        if response.status == 200:
            # Giả sử lấy được react_data ở đây
            content = await response.text()
            react_data = extract_react_data(content)
            whitelisted_data = extract_whitelist(react_data, KEYS_TO_KEEP)
            return "SUCCESS", {"id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat(), "method": "curl_cffi"}
        return f"CURL_HTTP_{response.status}", None
    except Exception as e:
        return f"CURL_ERR_{type(e).__name__}", None

async def fetch_aio(session, pid):
    """Xử lý với aiohttp request đơn lẻ và phân loại lỗi chi tiết."""
    url = CRAWL_URL_TEMPLATE.format(pid)
    
    headers = get_random_headers()
    try:
        async with session.get(url, headers=headers, timeout=12) as response:
            if response.status == 200:
                html = await response.text()
                react_data = extract_react_data(html)
                whitelisted_data = extract_whitelist(react_data, KEYS_TO_KEEP)

                return "SUCCESS", {"id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat(), "method": "aiohttp"}
            return f"AIO_HTTP_{response.status}", None
    except Exception as e:
        return f"AIO_ERR_{type(e).__name__}", None

async def fetch_with_aiohttp(session, pid):
    """Xử lý với aiohttp request đơn lẻ và phân loại lỗi chi tiết."""
    url = CRAWL_URL_TEMPLATE.format(pid)
    
    headers = get_random_headers()

    start_time = time.perf_counter()
    try:
        async with session.get(url, headers=headers, timeout=12) as response:
            duration = time.perf_counter() - start_time
            if response.status == 200:
                html = await response.text()
                react_data = extract_react_data(html)
                whitelisted_data = extract_whitelist(react_data, KEYS_TO_KEEP)

                return "SUCCESS", {"product_id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat(), "method": "aiohttp"}, duration
            return f"AIO_HTTP_{response.status}", None, duration
    except Exception as e:
        return f"AIO_ERR_{type(e).__name__}", None, time.perf_counter() - start_time

# Hàm cũ, không dùng nữa
async def fetch_product(session, pid):
    """Xử lý với aiohttp request đơn lẻ và phân loại lỗi chi tiết."""
    url = CRAWL_URL_TEMPLATE.format(pid)
    
    headers = get_random_headers()
    try:
        async with session.get(url, headers=headers, timeout=12) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                script_tag = soup.find('script', string=re.compile('react_data'))
                    
                if script_tag and script_tag.string:
                    match = re.search(r'react_data\s*[:=]\s*({.+?});', script_tag.string, re.DOTALL)
                    if match:
                        data_json = json.loads(match.group(1))
                        cleaned_data = filter_json(data_json, KEYS_TO_REMOVE)
                        whitelisted_data = extract_whitelist(cleaned_data, KEYS_TO_KEEP)
                            
                        record = {"product_id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat()}
                        async with aiofiles.open(JSONL_FILE, mode='a', encoding='utf-8') as f:
                            await f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        return "SUCCESS"
                return "MISSING_DATA"
                
            elif response.status == 403: return "403_FORBIDDEN"
            elif response.status == 404: return "404_NOT_FOUND"
            else: return f"HTTP_{response.status}"

        # response = await session.get(url, timeout=12) # Thư viện curl_cffi
        # if response.status == 200:
        #     html = await response.text()
        #     soup = BeautifulSoup(html, 'lxml')
        #     script_tag = soup.find('script', string=re.compile('react_data'))
                
        #     if script_tag and script_tag.string:
        #         match = re.search(r'react_data\s*[:=]\s*({.+?});', script_tag.string, re.DOTALL)
        #         if match:
        #             data_json = json.loads(match.group(1))
        #             cleaned_data = filter_json(data_json, KEYS_TO_REMOVE)
        #             whitelisted_data = extract_whitelist(cleaned_data, KEYS_TO_KEEP)
        #             record = {"product_id": pid, "data": whitelisted_data, "crawled_at": datetime.now().isoformat()}
        #             async with aiofiles.open(JSONL_FILE, mode='a', encoding='utf-8') as f:
        #                 await f.write(json.dumps(record, ensure_ascii=False) + "\n")
        #             return "SUCCESS"
        #     return "MISSING_DATA"
            
        # elif response.status == 403: return "403_FORBIDDEN"
        # elif response.status == 404: return "404_NOT_FOUND"
        # else: return f"HTTP_{response.status}"
            
    except asyncio.TimeoutError: return "TIMEOUT"
    except Exception as e: return f"ERROR_{type(e).__name__}"

# Không dùng nữa
async def crawl_manager(initial_products):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    global_errors = {}
    current_list = initial_products
    history_stats = []

    async with aiohttp.ClientSession() as session:
        for attempt in range(1, 7):
            label = "CHẠY CHÍNH" if attempt == 1 else f"RETRY {attempt-1}"
            print(f"[{datetime.now().strftime('%H:%M:%S')}] --- BẮT ĐẦU {label} ({len(current_list)} SP) ---")
            
            round_start = time.perf_counter()
            round_success = 0
            
            for i in range(0, len(current_list), BATCH_SIZE):
                batch_start = time.perf_counter()
                batch = current_list[i : i + BATCH_SIZE]
                tasks = [fetch_product_data(session, p['product_id'], semaphore) for p in batch]
                results = await asyncio.gather(*tasks)
                
                for idx, res in enumerate(results):
                    p_id = batch[idx]['product_id']
                    if res == "SUCCESS":
                        round_success += 1
                        if p_id in global_errors: del global_errors[p_id]
                    else:
                        global_errors[p_id] = {"product_id": p_id, "error_type": res, "last_attempt": label}
                
                print(f"   > Batch {i//BATCH_SIZE + 1}: {min(i+BATCH_SIZE, len(current_list))}/{len(current_list)} | Thành công: {round_success} | Time: {time.perf_counter()-batch_start:.2f}s")

            # CẬP NHẬT FILE LỖI (Smart Update - Không ghi đè sản phẩm chưa chạy)
            with open(FAILED_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['product_id', 'error_type', 'last_attempt'])
                writer.writeheader()
                writer.writerows(global_errors.values())

            duration = time.perf_counter() - round_start
            print(f"[{datetime.now().strftime('%H:%M:%S')}] KẾT THÚC {label}. Lỗi tồn đọng: {len(global_errors)}")
            
            history_stats.append({"round": label, "total": len(current_list), "success": round_success, "failed": len(global_errors), "time": duration})

            if not global_errors or attempt == 6: break
            current_list = [{"product_id": pid} for pid in global_errors.keys()]
            
            # NGHỈ GIỮA CÁC ATTEMPT ĐỂ IP ĐƯỢC GIẢI NHIỆT
            print("--> Đang nghỉ 5s để tránh bị block tiếp...")
            await asyncio.sleep(5)

    return history_stats

# --- HÀM GHI FILE AN TOÀN (ATOMIC OPERATIONS) ---

async def safe_save_jsonl(data, stats):
    async with file_lock:
        # Tính toán chính xác file index dựa trên số lượng thực tế đã ghi
        file_index = stats['total_success'] // RECORDS_PER_FILE
        file_path = os.path.join(JSONL_FILE.format(file_index))
        
        async with open(file_path, mode='a', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")
        
        # Chỉ tăng biến đếm SAU KHI ghi thành công vào file
        stats['total_success'] += 1

async def safe_update_csv(file_name, data):
    async with file_lock:
        file_path = os.path.join(LOG_DIR, file_name)
        file_exists = os.path.isfile(file_path)

        with open(file_path, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=data.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(data)

# --- ENGINE ---

async def run_phase(pids, phase_name, fetch_func, workers_count, shared_stats, extra_arg):
    phase_start = time.time()
    current_ids = list(pids)
    failed_details = {} # Lưu lỗi cuối cùng của mỗi ID

    for attempt in range(1, MAX_RETRIES + 1):
        if not current_ids: break
        
        retry_start = time.time()
        queue = asyncio.Queue()
        for i in current_ids: await queue.put(i)
        
        next_retry_ids = []
        attempt_win = 0
        attempt_fail = 0

        async def worker():
            nonlocal attempt_win, attempt_fail
            while not queue.empty():
                pid = await queue.get()
                res, data = await fetch_func(extra_arg, pid)
                
                if res == "SUCCESS":
                    attempt_win += 1
                    await safe_save_jsonl(data, shared_stats)
                    await safe_update_csv("success.csv", {
                        "id": pid, "phase": phase_name, "attempt": attempt, "time": datetime.now()
                    })
                else:
                    attempt_fail += 1
                    next_retry_ids.append(pid)
                    failed_details[pid] = res # Cập nhật lỗi mới nhất

                # Thống kê batch
                if (attempt_win + attempt_fail) % BATCH_SIZE == 0:
                    print(f"   [{phase_name}] R{attempt} Progress: {attempt_win + attempt_fail} SP...")
                
                queue.task_done()

        workers = [asyncio.create_task(worker()) for _ in range(workers_count)]
        await queue.join()
        for w in workers: w.cancel()
        
        print(f">> {phase_name} R{attempt} Done: Win {attempt_win}, Fail {attempt_fail}, Took {time.time()-retry_start:.2f}s")
        current_ids = next_retry_ids

    # Sau khi hết 3 lần retry, mới ghi những ID thực sự thất bại vào CSV
    for pid in current_ids:
        await safe_update_csv("failed.csv", {
            "id": pid, "phase": phase_name, "final_error": failed_details.get(pid), "time": datetime.now()
        })
    
    return current_ids

async def main_pipeline_new(all_pids):
    shared_stats = {'total_success': 0}
    global_start = time.time()

    # PHASE 1: PLAYWRIGHT
    print(f"\n--- BẮT ĐẦU PHASE 1: PLAYWRIGHT ({len(all_pids)} SP) ---")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        remaining = await run_phase(all_pids, "playwright", fetch_pw, NUM_PW_WORKERS, shared_stats, context)
        await browser.close()

    # PHASE 2: CURL_CFFI (Hỗ trợ vượt 403 tốt hơn)
    if remaining:
        print(f"\n--- BẮT ĐẦU PHASE 2: CURL_CFFI ({len(remaining)} SP) ---")
        async with AsyncSession() as session:
            final_failed_pids = await run_phase(remaining, "curl_cffi", fetch_curl, NUM_CURL_WORKERS, shared_stats, session)
    else:
        final_failed_pids = []

    print(f"\n" + "="*30)
    print(f"TỔNG KẾT TOÀN BỘ QUÁ TRÌNH:")
    print(f"- Tổng thời gian: {time.time() - global_start:.2f}s")
    print(f"- Tổng thành công: {shared_stats['total_success']}")
    print(f"- Tổng thất bại cuối cùng: {len(final_failed_pids)}")
    print("="*30)

async def main_pipeline(all_pids):
    shared_stats = {'total_success': 0}
    global_start = time.time()

    # PHASE 1: PLAYWRIGHT
    print(f"\n--- BẮT ĐẦU PHASE 1: PLAYWRIGHT ({len(all_pids)} SP) ---")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        remaining_pids = await run_phase(all_pids, "playwright", fetch_pw, NUM_PW_WORKERS, shared_stats, context)
        await browser.close()

    # PHASE 2: AIOHTTP (Chỉ chạy với remaining_pids)
    if remaining_pids:
        print(f"\n--- BẮT ĐẦU PHASE 2: AIOHTTP ({len(remaining_pids)} SP) ---")
        async with aiohttp.ClientSession() as session:
            final_failed_pids = await run_phase(remaining_pids, "aiohttp", fetch_aio, NUM_AIO_WORKERS, shared_stats, session)
    else:
        final_failed_pids = []

    print(f"\n" + "="*30)
    print(f"TỔNG KẾT TOÀN BỘ QUÁ TRÌNH:")
    print(f"- Tổng thời gian: {time.time() - global_start:.2f}s")
    print(f"- Tổng thành công: {shared_stats['total_success']}")
    print(f"- Tổng thất bại cuối cùng: {len(final_failed_pids)}")
    print("="*30)

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
        return [p['product_id'] for p in products]
    except Exception as e:
        print(f"✗ Lỗi khi kết nối hoặc truy vấn MongoDB: {e}")
        return []


if __name__ == "__main__":
    # Lấy product_ids từ temp collection rồi crawl
    product_ids = get_product_ids_from_db()
    print(f"Tìm thấy {len(product_ids)} sản phẩm cần crawl.")
    # asyncio.run(main_pipeline_new(product_ids))

    # Chạy tuần tự để lấy product_ids rồi mới crawl
    # products, prep_time = prepare_data(), 0 # Lấy unique product ids và thời gian chuẩn bị
    # if products:
        # product_ids = [p['product_id'] for p in products]
        # product_ids = get_product_ids_from_db()
        # asyncio.run(main_pipeline(product_ids))
        # asyncio.run(run_crawl(product_ids))
        # stats = asyncio.run(crawl_manager(products))
        # print_report(prep_time, stats, len(products))