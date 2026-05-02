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
from datetime import datetime, UTC
import random
from curl_cffi.requests import AsyncSession

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
JSONL_FILE = f"{OUTPUT_DIR}/product_data.jsonl"
SUCCESS_CSV = f"{OUTPUT_DIR}/unique_product_ids.csv"
FAILED_CSV = f"{OUTPUT_DIR}/failed_product_ids.csv"
CRAWL_URL_TEMPLATE = "https://www.glamira.vn/catalog/product/view/id/{}"
# GIẢM CONCURRENCY ĐỂ TRÁNH BỊ CHẶN IP NHANH
MAX_CONCURRENT_REQUESTS = 15
# BATCH_SIZE = 100
RATE_LIMIT = 50 # SỐ REQUEST TỐI ĐA / GIÂY

# DANH SÁCH USER-AGENTS ĐỂ XOAY VÒNG
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1"
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

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

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


async def fetch_product(session, pid):
    """Xử lý request đơn lẻ và phân loại lỗi chi tiết."""
    url = CRAWL_URL_TEMPLATE.format(pid)
    try:
        headers = get_random_headers()

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
                            
                        record = {"product_id": pid, "data": cleaned_data, "crawled_at": datetime.now().isoformat()}
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
                        
        #             record = {"product_id": pid, "data": cleaned_data, "crawled_at": datetime.now().isoformat()}
        #             async with aiofiles.open(JSONL_FILE, mode='a', encoding='utf-8') as f:
        #                 await f.write(json.dumps(record, ensure_ascii=False) + "\n")
        #             return "SUCCESS"
        #     return "MISSING_DATA"
            
        # elif response.status == 403: return "403_FORBIDDEN"
        # elif response.status == 404: return "404_NOT_FOUND"
        # else: return f"HTTP_{response.status}"
            
    except asyncio.TimeoutError: return "TIMEOUT"
    except Exception as e: return f"ERROR_{type(e).__name__}"

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

# --- PHẦN 3: BÁO CÁO ---
def print_report(prep_time, stats, total_sp):
    print("\n" + "="*75)
    print(f"{'BÁO CÁO CHI TIẾT TỪNG ĐỢT CHẠY':^75}")
    print("="*75)
    print(f"{'Giai đoạn':<15} | {'Xử lý':<10} | {'Thành công':<12} | {'Lỗi tồn đọng':<12} | {'Thời gian':<10}")
    print("-" * 75)
    total_success = 0
    total_crawl_time = 0
    for s in stats:
        print(f"{s['round']:<15} | {s['total']:<10} | {s['success']:<12} | {s['failed']:<12} | {s['time']:>8.2f}s")
        total_success += s['success']
        total_crawl_time += s['time']
    print("-" * 75)
    print(f"Kết quả cuối cùng: {total_success}/{total_sp} sản phẩm thành công.")
    print(f"Tổng thời gian: {prep_time + total_crawl_time:.2f}s")
    print("="*75)

async def worker(name, queue, session, stats, failed_list):
    """Công nhân lấy việc từ Queue và cập nhật thống kê thời gian thực."""
    while True:
        # Lấy dữ liệu từ Queue (gồm ID và số lần đã thử)
        item = await queue.get()
        p_id = item['id']
        retries = item['retries']

        try:
            p_id = await queue.get()
            result = await fetch_product(session, p_id)
            
            if result == "SUCCESS":
                stats['success'] += 1
            else:
                if retries < 3: # Số lần thử tối đa
                    print(f"   [Retry] ID {p_id} thất bại ({result}). Thử lại lần {retries + 1}...")
                    await queue.put({'id': p_id, 'retries': retries + 1})
                else:
                    stats['failed'] += 1
                    failed_list.append({"product_id": p_id, "error_type": result, "time": datetime.now().strftime('%H:%M:%S')})
            
            # Log tiến độ mỗi khi xong 1 sản phẩm (tùy chọn ẩn nếu quá nhiều)
            if (stats['success'] + stats['failed']) % 50 == 0:
                print(f"   > [Progress] {stats['success'] + stats['failed']} SP - Success: {stats['success']} | Failed: {stats['failed']}")
        except Exception as e:
            print(f"Lỗi ở phần worker p_id {p_id}: {e}")
        finally:
            queue.task_done()
        
        # Thêm Jitter nhỏ để các worker không dẫm chân nhau
        # await asyncio.sleep(float(1/RATE_LIMIT))
        await asyncio.sleep(random.uniform(0.1, 0.4))

async def run_crawl(product_ids):
    """Quản lý vòng đời của Queue và Workers."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] --- KHỞI CHẠY HỆ THỐNG QUEUE ({len(product_ids)} SP) ---")

    queue = asyncio.Queue()
    for pid in product_ids:
        await queue.put(pid)
    
    stats = {'success': 0, 'failed': 0}
    failed_list = []
    start_time = time.perf_counter()

    # async with AsyncSession(impersonate="chrome110") as session: # Thư viện curl_cffi
    async with aiohttp.ClientSession() as session:
        # Tạo nhóm worker
        tasks = []
        for i in range(MAX_CONCURRENT_REQUESTS):
            task = asyncio.create_task(worker(f"W-{i}", queue, session, stats, failed_list))
            tasks.append(task)

        # Chờ Queue xử lý xong hết
        await queue.join()

        # Dừng các worker
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    # Ghi file lỗi cuối cùng
    import csv
    with open(FAILED_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['product_id', 'error_type', 'time'])
        writer.writeheader()
        writer.writerows(failed_list)

    duration = time.perf_counter() - start_time
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] --- HOÀN THÀNH CRAWL DATA ---")
    print(f" - Tổng thời gian: {duration:.2f} giây")
    print(f" - Thành công: {stats['success']}")
    print(f" - Thất bại: {stats['failed']} (Chi tiết tại {FAILED_CSV})")
    print(f" - Tốc độ trung bình: {len(product_ids)/duration:.2f} SP/giây")

if __name__ == "__main__":
    products, prep_time = prepare_data(), 0 # Sửa nhẹ để chạy mẫu
    if products:
        product_ids = [p['product_id'] for p in products]
        asyncio.run(run_crawl(product_ids))
        # stats = asyncio.run(crawl_manager(products))
        # print_report(prep_time, stats, len(products))