import pymongo
import csv
import time
import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

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
LOG_DIR = "output/logs"
SUCCESS_CSV = f"{OUTPUT_DIR}/unique_product_ids.csv"
FAILED_CSV = f"{OUTPUT_DIR}/failed_product_ids.csv"
CRAWL_URL_TEMPLATE = "https://www.glamira.vn/catalog/product/view/id/{}"

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

if __name__ == "__main__":
    prepare_data()
