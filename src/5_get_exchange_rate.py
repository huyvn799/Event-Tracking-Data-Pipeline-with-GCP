import pandas as pd
import requests
import time
from datetime import datetime

INPUT_CSV = 'input/currencies_full.csv'
OUTPUT_CSV = 'output/exchange_rates_2019_2020.csv'
BASE_URL = "https://fxapi.app/api/history/usd/{currency}.json"

DATE_PERIODS = [
    {"from": "2019-01-01", "to": "2019-12-31"},
    {"from": "2020-01-01", "to": "2020-12-31"}
]

final_data = []

# 1. Đọc file CSV đầu vào và làm sạch dữ liệu
try:
    df_input = pd.read_csv(INPUT_CSV)
    df_input['currency_code'] = df_input['currency_code'].astype(str).str.strip()
    currency_codes = df_input['currency_code'].dropna().unique()
    print(f"Tìm thấy {len(currency_codes)} mã tiền tệ: {list(currency_codes)}")
except Exception as e:
    print(f"Lỗi đọc file CSV: {e}")
    currency_codes = []

# 2. Vòng lặp lấy dữ liệu từ API
for currency in currency_codes:
    currency_formatted = currency.lower() 
    print(f"\n--- Processing: {currency_formatted.upper()} ---")
    
    for period in DATE_PERIODS:
        url = BASE_URL.format(currency=currency_formatted)
        params = {
            "from": period["from"],
            "to": period["to"]
        }
        
        try:
            response = requests.get(url, params=params, timeout=10)
            print(f"Request URL: {response.url}")
            
            if response.status_code == 200:
                api_data = response.json()
                
                # THAY ĐỔI QUAN TRỌNG Ở ĐÂY:
                # rates_list bây giờ là một LIST chứa các object (dict trong Python)
                rates_list = api_data.get('rates', []) 
                
                # Duyệt qua từng object trong list
                for item in rates_list:
                    # Lấy giá trị của 2 key 'date' và 'rate' từ object
                    date_str = item.get('date')
                    rate = item.get('rate')
                    
                    # Nếu thiếu 1 trong 2 thông tin thì bỏ qua dòng này
                    if not date_str or rate is None:
                        continue
                        
                    # Chuyển đổi định dạng ngày từ YYYY-MM-DD sang YYYYMMDD
                    try:
                        date_key = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y%m%d")
                    except ValueError:
                        continue # Bỏ qua nếu chuỗi ngày không đúng định dạng
                    
                    # Ghi nhận dữ liệu vào danh sách tổng hợp
                    final_data.append({
                        "date_key": date_key,
                        "from_currency_code": currency.upper(),
                        "to_currency_code": "USD",
                        "exchange_rate": rate
                    })
                print(f"-> Thành công! Lấy được {len(rates_list)} dòng dữ liệu.")
                
            elif response.status_code == 404:
                print(f"-> LỖI 404: Vui lòng kiểm tra lại URL hoặc mã tiền tệ.")
            else:
                print(f"-> Lỗi hệ thống: Mã lỗi {response.status_code}")
                
        except Exception as e:
            print(f"-> Lỗi kết nối/xử lý: {e}")
        
        # Rate limiting: Nghỉ 1 giây giữa các request
        time.sleep(1) 

# 3. Xuất dữ liệu ra file CSV kết quả
if final_data:
    df_output = pd.DataFrame(final_data)
    df_output = df_output[["date_key", "from_currency_code", "to_currency_code", "exchange_rate"]]
    
    # Sắp xếp lại dữ liệu theo mã tiền tệ và ngày tăng dần
    df_output = df_output.sort_values(by=["from_currency_code", "date_key"])
    
    df_output.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    print(f"\n Hoàn tất! Dữ liệu đã được xuất ra file '{OUTPUT_CSV}'")
    print(f"Tổng số dòng ghi nhận: {len(df_output)}")
else:
    print("\n Không có dữ liệu nào được trích xuất.")