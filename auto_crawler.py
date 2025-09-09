import os
import sys
from datetime import datetime
import schedule
import time

# Thêm đường dẫn
sys.path.append('d:/Jupyter notebook/KingfoodMart')

from crawl_kf import fetch_and_save_products

def load_categories():
    """Đọc danh sách categories từ file txt"""
    categories = []
    try:
        with open('d:/Jupyter notebook/KingfoodMart/category_url.txt', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and line.startswith('https://'):  # Chỉ lấy URL hợp lệ
                    categories.append(line)
        print(f"📁 Loaded {len(categories)} categories")
        return categories
    except Exception as e:
        print(f"❌ Error loading categories: {e}")
        return []

def extract_slug_from_url(url):
    """Trích xuất slug từ URL"""
    # https://kingfoodmart.com/hang-thung-gia-tot -> hang-thung-gia-tot
    slug = url.split('/')[-1]
    return slug

def daily_crawl_job():
    """Crawl tất cả categories từ file"""
    print(f"🕙 Starting daily crawl at {datetime.now()}")
    
    categories = load_categories()
    
    for i, category_url in enumerate(categories, 1):
        try:
            slug = extract_slug_from_url(category_url)
            
            print(f"📂 [{i}/{len(categories)}] Crawling: {slug}")
            
            fetch_and_save_products(
                start_page=1,
                end_page=5,
                limit_value=102,
                slug_value=slug,
                target_date=datetime.now()
            )
            
            print(f"✅ Completed: {slug}")
            time.sleep(3)  # Nghỉ 3s giữa các category để tránh spam
            
        except Exception as e:
            print(f"❌ Error crawling {slug}: {e}")
            continue  # Tiếp tục với category tiếp theo
    
    print(f"🎉 All categories completed at {datetime.now()}")

def main():
    """Hàm chính để chạy lập lịch"""
    print("🤖 Starting automated crawler scheduler...")
    
    # Lập lịch chạy hàng ngày lúc 10h tối
    schedule.every().day.at("22:00").do(daily_crawl_job)
    
    # Chạy ngay lần đầu để test
    print("🧪 Running initial test...")
    daily_crawl_job()
    
    print("⏰ Scheduler started. Waiting for scheduled tasks...")
    
    # Vòng lặp chính
    while True:
        schedule.run_pending()
        time.sleep(60)  # Kiểm tra mỗi phút

if __name__ == "__main__":
    main()