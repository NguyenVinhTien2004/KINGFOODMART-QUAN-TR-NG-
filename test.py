import os
import requests
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error
import json
import time

load_dotenv()
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
url = os.getenv("API_URL")

# Kết nối MySQL
def create_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database="kfm",
            user='root',
            password="123456789@"  
        )
        if connection.is_connected():
            print("Kết nối MySQL thành công")
            return connection
    except Error as e:
        print(f"Lỗi kết nối MySQL: {e}")
        return None

# SIMPLIFIED STOCK CALCULATION FUNCTION với UNIQUE KEY
def simple_stock_history_calculation(connection, product_id, current_stock, target_date):
    """
    Tính toán stock_history đơn giản - chỉ lưu stock_quantity cho mỗi ngày
    Sử dụng INSERT ... ON DUPLICATE KEY UPDATE cho hiệu suất tốt hơn
    """
    cursor = connection.cursor()
    target_date_str = target_date.strftime("%Y-%m-%d")
    
    try:
        print(f"🔄 Processing stock for {product_id} on {target_date_str}")
        
        # Lấy thời gian hiện tại để insert
        current_time = datetime.now().strftime("%H:%M:%S")
        
        # Sử dụng INSERT ... ON DUPLICATE KEY UPDATE với current_time
        cursor.execute("""
            INSERT INTO stock_history (product_id, date, stock_quantity, created_at)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            stock_quantity = VALUES(stock_quantity),
            created_at = VALUES(created_at)
        """, (product_id, target_date_str, current_stock, current_time))
        
        if cursor.rowcount == 1:
            print(f"🆕 Created new stock record: {current_stock}")
            return True
        elif cursor.rowcount == 2:
            print(f"✅ Updated stock record: {current_stock}")
            return True
        else:
            print(f"⚖️ No stock change needed: {current_stock}")
            return False
        
    except Exception as e:
        print(f"❌ Error in stock calculation for {product_id}: {e}")
        return False
    
    finally:
        cursor.close()

def create_simplified_stock_history_table(connection):
    """
    Tạo bảng stock_history đơn giản chỉ với các trường cần thiết
    Với UNIQUE KEY constraint và created_at chỉ lưu TIME
    """
    try:
        cursor = connection.cursor()
        
        # Kiểm tra nếu bảng đã tồn tại
        cursor.execute("SHOW TABLES LIKE 'stock_history'")
        table_exists = cursor.fetchone()
        
        if not table_exists:
            # Tạo bảng mới với created_at kiểu TIME và NOT NULL
            cursor.execute("""
            CREATE TABLE stock_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                product_id VARCHAR(255) NOT NULL,
                date DATE NOT NULL,
                stock_quantity INT DEFAULT 0,
                created_at TIME NOT NULL,
                UNIQUE KEY unique_product_date (product_id, date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """)
            print("✅ Created simplified stock_history table with UNIQUE KEY")
        else:
            # Kiểm tra nếu UNIQUE KEY đã tồn tại
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.table_constraints 
                WHERE table_name = 'stock_history' 
                AND constraint_type = 'UNIQUE'
                AND table_schema = DATABASE()
            """)
            has_unique_key = cursor.fetchone()[0]
            
            if not has_unique_key:
                # Thêm UNIQUE KEY nếu chưa có
                cursor.execute("""
                    ALTER TABLE stock_history 
                    ADD UNIQUE KEY unique_product_date (product_id, date)
                """)
                print("✅ Added UNIQUE KEY to existing stock_history table")
            
            # Kiểm tra và sửa created_at nếu có thể NULL
            cursor.execute("""
                SELECT IS_NULLABLE FROM information_schema.COLUMNS 
                WHERE table_name = 'stock_history' 
                AND column_name = 'created_at'
                AND table_schema = DATABASE()
            """)
            nullable_result = cursor.fetchone()
            
            if nullable_result and nullable_result[0] == 'YES':
                # Cập nhật các giá trị NULL thành thời gian hiện tại
                current_time = datetime.now().strftime("%H:%M:%S")
                cursor.execute("""
                    UPDATE stock_history 
                    SET created_at = %s 
                    WHERE created_at IS NULL
                """, (current_time,))
                
                # Thay đổi cột thành NOT NULL
                cursor.execute("""
                    ALTER TABLE stock_history 
                    MODIFY COLUMN created_at TIME NOT NULL
                """)
                print("✅ Updated stock_history.created_at to NOT NULL")
            else:
                print("✅ Stock_history table already has proper constraints")
        
        connection.commit()
        
    except Error as e:
        print(f"❌ Error creating/updating table: {e}")
    finally:
        cursor.close()

def integrated_stock_processing(connection, product_id, current_stock, target_date):
    """
    Code tích hợp để thay thế phần stock processing trong fetch_and_save_products
    """
    
    # Tạo simplified table
    create_simplified_stock_history_table(connection)
    
    # Tính toán stock changes
    has_change = simple_stock_history_calculation(
        connection, product_id, current_stock, target_date
    )
    
    return has_change

# Tạo bảng nếu chưa tồn tại - ĐÃ SỬA
def create_tables(connection):
    try:
        cursor = connection.cursor()
        
        # Bảng product - created_at và updated_at chỉ lưu TIME và NOT NULL
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `product` (
            `product_id` VARCHAR(255) PRIMARY KEY,
            `category` VARCHAR(255),
            `name` TEXT,
            `price` BIGINT UNSIGNED DEFAULT 0,
            `promotion` TEXT,
            `date` DATE,
            `original_price` BIGINT UNSIGNED DEFAULT 0,
            `stock_quantity` INT DEFAULT 0,
            `total_sold` INT DEFAULT 0,
            `created_at` TIME NOT NULL,
            `updated_at` TIME NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Kiểm tra và sửa bảng product hiện có nếu created_at có thể NULL
        cursor.execute("SHOW TABLES LIKE 'product'")
        table_exists = cursor.fetchone()
        
        if table_exists:
            # Kiểm tra cột created_at có thể NULL không
            cursor.execute("""
                SELECT IS_NULLABLE FROM information_schema.COLUMNS 
                WHERE table_name = 'product' 
                AND column_name = 'created_at'
                AND table_schema = DATABASE()
            """)
            created_at_nullable = cursor.fetchone()
            
            cursor.execute("""
                SELECT IS_NULLABLE FROM information_schema.COLUMNS 
                WHERE table_name = 'product' 
                AND column_name = 'updated_at'
                AND table_schema = DATABASE()
            """)
            updated_at_nullable = cursor.fetchone()
            
            current_time = datetime.now().strftime("%H:%M:%S")
            
            if created_at_nullable and created_at_nullable[0] == 'YES':
                # Cập nhật các giá trị NULL thành thời gian hiện tại
                cursor.execute("""
                    UPDATE product 
                    SET created_at = %s 
                    WHERE created_at IS NULL
                """, (current_time,))
                
                # Thay đổi cột thành NOT NULL
                cursor.execute("""
                    ALTER TABLE product 
                    MODIFY COLUMN created_at TIME NOT NULL
                """)
                print("✅ Updated product.created_at to NOT NULL")
            
            if updated_at_nullable and updated_at_nullable[0] == 'YES':
                # Cập nhật các giá trị NULL thành thời gian hiện tại
                cursor.execute("""
                    UPDATE product 
                    SET updated_at = %s 
                    WHERE updated_at IS NULL
                """, (current_time,))
                
                # Thay đổi cột thành NOT NULL
                cursor.execute("""
                    ALTER TABLE product 
                    MODIFY COLUMN updated_at TIME NOT NULL
                """)
                print("✅ Updated product.updated_at to NOT NULL")
        
        # Bảng stock_history (đơn giản) - created_at chỉ lưu TIME và NOT NULL
        create_simplified_stock_history_table(connection)
        
        # Bảng price_history - created_at chỉ lưu TIME và NOT NULL
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS `price_history` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `product_id` VARCHAR(255) NOT NULL,
            `date` DATE NOT NULL,
            `price` BIGINT UNSIGNED DEFAULT 0,
            `original_price` BIGINT UNSIGNED DEFAULT 0,
            `created_at` TIME NOT NULL,
            UNIQUE KEY unique_product_date (product_id, date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Kiểm tra và sửa bảng price_history hiện có nếu created_at có thể NULL
        cursor.execute("SHOW TABLES LIKE 'price_history'")
        price_table_exists = cursor.fetchone()
        
        if price_table_exists:
            cursor.execute("""
                SELECT IS_NULLABLE FROM information_schema.COLUMNS 
                WHERE table_name = 'price_history' 
                AND column_name = 'created_at'
                AND table_schema = DATABASE()
            """)
            price_created_at_nullable = cursor.fetchone()
            
            if price_created_at_nullable and price_created_at_nullable[0] == 'YES':
                current_time = datetime.now().strftime("%H:%M:%S")
                
                # Cập nhật các giá trị NULL thành thời gian hiện tại
                cursor.execute("""
                    UPDATE price_history 
                    SET created_at = %s 
                    WHERE created_at IS NULL
                """, (current_time,))
                
                # Thay đổi cột thành NOT NULL
                cursor.execute("""
                    ALTER TABLE price_history 
                    MODIFY COLUMN created_at TIME NOT NULL
                """)
                print("✅ Updated price_history.created_at to NOT NULL")
        
        connection.commit()
        print("Các bảng đã được tạo/kiểm tra thành công")
        
    except Error as e:
        print(f"Lỗi tạo bảng: {e}")
    finally:
        if cursor:
            cursor.close()

headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

# Payload template sẽ được cập nhật động
payload_template = {
    "operationName": "ListingProductsBySlug",
    "query": """
    query ListingProductsBySlug($slug: String, $page: Int, $limit: Int, $filters: [ListingProductBySlugInput], $order: ListingProductSortEnum, $direction: OrderDirectionEnum) {
      listingProductsBySlug(
        slug: $slug
        page: $page
        limit: $limit
        filters: $filters
        order: $order
        direction: $direction
      ) {
        total
        page
        limit
        data {
          id
          discountPercent
          discountPrice
          thumbnail
          giftItems {
            id
            name
            thumbnail
            promotionInfo {
              id
              type
              name
              promotionSummary
              promotionApplyLimit
              __typename
            }
            __typename
          }
          images
          inStock
          isAlcohol
          name
          olClub {
            discountPrice
            discountPercent
            __typename
          }
          originalPrice
          slug
          thumbnail
          teasingInfo {
            hasDelivery
            openDate
            deliveryDate
            limitNote
            receivedNoti
            promotionId
            variantId
            productId
            __typename
          }
          itemTrait {
            itemType
            itemPromotion
            isTeasing
            __typename
          }
          variants {
            teasingInfo {
              hasDelivery
              openDate
              deliveryDate
              limitNote
              receivedNoti
              promotionId
              variantId
              productId
              __typename
            }
            itemTrait {
              itemType
              itemPromotion
              isTeasing
              __typename
            }
            id
            discountPercent
            discountPrice
            images
            name
            originalPrice
            price
            sku
            inStock
            thumbnail
            stockItem {
              quantity
              maxSaleQuantity
              minSaleQuantity
              __typename
            }
            unit {
              id
              name
              __typename
            }
            giftItems {
              id
              name
              thumbnail
              __typename
            }
            unitConversion {
              isBaseVariant
              baseUnitName
              pricePerBaseUnit
              conversion
              formatPricePerBaseUnit
              originalPricePerBaseUnit
              __typename
            }
            isOrdered
            isSelected
            slug
            isOnlineSale
            isSale
            preOrder {
              counter {
                ordered
                remain
                __typename
              }
              promotionDetail {
                id
                endAt
                deliveryDate
                termAndCondition {
                  title
                  content
                  __typename
                }
                __typename
              }
              deliveryDate
              __typename
            }
            groupBuy {
              levelPrice {
                level
                price
                costSavings
                isSelected
                discountTicker {
                  tickerId
                  position
                  isOverride
                  isImage
                  type
                  code
                  name
                  textColor
                  backgroundColor
                  strokeColor
                  imageUrl
                  deliveryDisplayText
                  __typename
                }
                __typename
              }
              incentivePercent
              groupCount
              promotionDetail {
                id
                endAt
                deliveryDate
                __typename
              }
              __typename
            }
            deliveryDate
            promotionInfoItems {
              id
              type
              name
              promotionSummary
              promotionApplyLimit
              __typename
            }
            promotionSummary
            promotionApplyLimit
            warnMsg {
              type
              message
              __typename
            }
            orderedCounter
            metadata
            hasOneInManyLimitation
            limitQuantity
            highlightedData {
              highlightedInfos {
                code
                text
                textColor
                backgroundColor
                fillColor
                fillRatio
                headingIcon
                boughtCustomerNames
                hoverable
                underlying
                gifts {
                  imageUrl
                  name
                  quantity
                  sku
                  isDisabled
                  badgeItem {
                    badgeId
                    position
                    isOverride
                    isImage
                    type
                    code
                    name
                    textColor
                    backgroundColor
                    strokeColor
                    imageUrl
                    deliveryDisplayText
                    __typename
                    }
                  __typename
                }
                __typename
              }
              __typename
            }
            __typename
          }
          isActive
          subCate
          tickerItems {
            tickerId
            isImage
            type
            code
            name
            textColor
            backgroundColor
            strokeColor
            imageUrl
            __typename
          }
          badgeItems {
            badgeId
            isImage
            type
            code
            name
            textColor
            backgroundColor
            strokeColor
            imageUrl
            __typename
          }
          __typename
        }
        cateId
        subCateId
        specCateId
        brandId
        __typename
      }
    }
    """,
    "variables": {
        "limit": 102,
        "page": 1,
        "slug": "",
        "filters": []
    }
}

def fetch_and_save_products(start_page, end_page=None, limit_value=102, slug_value="", target_date=None):
    """
    Hàm thu thập và lưu sản phẩm từ API vào MySQL - ĐÃ SỬA
    - start_page: trang bắt đầu
    - end_page: trang kết thúc (None để lấy đến khi hết dữ liệu)
    - limit_value: số sản phẩm mỗi trang
    - slug_value: slug của danh mục
    - target_date: ngày mục tiêu để lưu dữ liệu (nếu None sẽ dùng ngày hiện tại)
    """
    connection = create_connection()
    if not connection:
        print("Không thể kết nối đến MySQL")
        return
    
    create_tables(connection)
    
    # Sử dụng ngày mục tiêu nếu được cung cấp, nếu không dùng ngày hiện tại
    if target_date is None:
        target_date = datetime.now()
    
    target_date_str = target_date.strftime("%Y-%m-%d")
    current_time = datetime.now().strftime("%H:%M:%S")  # Lấy giờ hiện tại
    
    total_products = 0
    page = start_page
    has_more_data = True
    max_retries = 3
    
    while has_more_data and (end_page is None or page <= end_page):
        # Tạo payload mới cho mỗi trang
        payload = {
            "operationName": "ListingProductsBySlug",
            "query": payload_template["query"],
            "variables": {
                "limit": limit_value,
                "page": page,
                "slug": slug_value,
                "filters": []
            }
        }
        
        retry_count = 0
        success = False
        
        while retry_count < max_retries and not success:
            try:
                print(f"Fetching data from page {page} for slug '{slug_value}'...")
                response = requests.post(url, headers=headers, json=payload, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Kiểm tra nếu API trả về lỗi
                    if "errors" in data:
                        print(f"API returned errors: {data['errors']}")
                        break
                    
                    products = data.get("data", {}).get("listingProductsBySlug", {}).get("data", [])
                    
                    # Kiểm tra nếu không còn sản phẩm thì dừng
                    if not products:
                        print(f"Trang {page} không có dữ liệu, dừng lại.")
                        has_more_data = False
                        break
                    
                    # Lưu response để debug (tùy chọn)
                    with open(f"response_{slug_value}_page_{page}.json", "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=2)
                    
                    cursor = connection.cursor()
                    page_products = 0
                    
                    for product in products:
                        print(f"Processing product for date: {target_date_str}")
                        
                        description = product.get("descriptionJson", {}).get("introduction", "") if product else ""
                        
                        giftItems = product.get("giftItems") or []
                        promotion = "Không có khuyến mãi"
                        
                        if giftItems:
                            promotion_texts = []
                            for gift_item in giftItems:
                                promotion_info = gift_item.get("promotionInfo")
                                if promotion_info:
                                    promo_text = promotion_info.get("promotionSummary", "Khuyến mãi")
                                    promotion_texts.append(promo_text)
                            
                            if promotion_texts:
                                promotion = ", ".join(promotion_texts)
                        
                        product_variants = product.get("variants", [])
                        
                        for variant in product_variants:
                            try:
                                product_id = variant["id"]
                                total_sold = variant.get("orderedCounter", 0)
                                stock_quantity = int(variant.get("stockItem", {}).get("quantity", 0))
                                original_price = variant.get("originalPrice", 0)
                                price = variant.get("discountPrice", original_price)
                                product_name = variant.get("name", "Unknown Product")
                                
                                # Validate and sanitize data
                                product_name = str(product_name) if product_name else "Unknown"
                                stock_quantity = max(0, int(stock_quantity)) if stock_quantity is not None else 0
                                total_sold = max(0, int(total_sold)) if total_sold is not None else 0
                                price = max(0, int(price)) if price is not None else 0
                                original_price = max(0, int(original_price)) if original_price is not None else 0
                                promotion = str(promotion) if promotion else "Không có khuyến mãi"
                                
                                # Lấy thời gian hiện tại cho mỗi sản phẩm
                                current_time = datetime.now().strftime("%H:%M:%S")
                                
                                # 1. Sử dụng INSERT ... ON DUPLICATE KEY UPDATE để xử lý cả insert và update
                                cursor.execute("""
                                    INSERT INTO product (product_id, name, stock_quantity, total_sold, 
                                        price, original_price, promotion, category, date, created_at, updated_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        name = VALUES(name),
                                        stock_quantity = VALUES(stock_quantity),
                                        total_sold = VALUES(total_sold),
                                        price = VALUES(price),
                                        original_price = VALUES(original_price),
                                        promotion = VALUES(promotion),
                                        category = VALUES(category),
                                        date = VALUES(date),
                                        updated_at = VALUES(updated_at)
                                """, (product_id, product_name, stock_quantity, total_sold, 
                                     price, original_price, promotion, slug_value, target_date_str, 
                                     current_time, current_time))
                                
                                # 2. Xử lý lịch sử giá với INSERT ... ON DUPLICATE KEY UPDATE
                                cursor.execute("""
                                    INSERT INTO price_history (product_id, date, price, original_price, created_at)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE 
                                    price = VALUES(price), 
                                    original_price = VALUES(original_price),
                                    created_at = VALUES(created_at)
                                """, (product_id, target_date_str, price, original_price, current_time))
                                
                                # 3. Xử lý lịch sử kho - ĐƠN GIẢN
                                has_change = integrated_stock_processing(
                                    connection, product_id, stock_quantity, target_date
                                )
                                
                                if has_change:
                                    print(f"Stock updated for {product_id}: {stock_quantity}")
                                else:
                                    print(f"No stock change for {product_id}")
                                
                                page_products += 1
                                total_products += 1
                                
                                if page_products % 10 == 0:
                                    print(f"  Processed {page_products} products on page {page}")
                                    
                            except Error as e:
                                print(f"Lỗi khi xử lý sản phẩm {product_id}: {e}")
                                connection.rollback()
                            except KeyError as e:
                                print(f"Thiếu trường dữ liệu trong variant: {e}")
                                continue
                            except Exception as e:
                                print(f"Lỗi không xác định khi xử lý sản phẩm {product_id}: {e}")
                                continue
                    
                    cursor.close()
                    connection.commit()
                    print(f"Page {page} completed: {page_products} products processed")
                    success = True
                    
                else:
                    print(f"Request for page {page} failed with status code {response.status_code}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Retrying... ({retry_count}/{max_retries})")
                        time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                print(f"Network error on page {page}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying... ({retry_count}/{max_retries})")
                    time.sleep(3)
            except json.JSONDecodeError as e:
                print(f"JSON decode error on page {page}: {e}")
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Retrying... ({retry_count}/{max_retries})")
                    time.sleep(2)
        
        if not success:
            print(f"Failed to process page {page} after {max_retries} attempts. Moving to next page.")
        
        page += 1
        time.sleep(1)
    
    connection.close()
    print(f"\nTotal number of products retrieved for '{slug_value}': {total_products}")
    return total_products

# UTILITY FUNCTIONS MỚI
def calculate_daily_stock_changes(connection, product_id, days=7):
    """
    Tính toán thay đổi stock hàng ngày bằng cách so sánh stock_quantity giữa các ngày
    """
    cursor = connection.cursor()
    
    try:
        cursor.execute("""
            SELECT date, stock_quantity
            FROM stock_history 
            WHERE product_id = %s 
                AND date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
            ORDER BY date ASC
        """, (product_id, days))
        
        results = cursor.fetchall()
        
        if results and len(results) > 1:
            print(f"\n📊 Daily Stock Changes for {product_id} (Last {days} days):")
            print("=" * 50)
            print(f"{'Date':<12} {'Stock':<8} {'Change':<8} {'Change %':<10}")
            print("-" * 50)
            
            previous_stock = None
            previous_date = None
            
            for row in results:
                current_date, current_stock = row
                
                if previous_stock is not None:
                    stock_change = current_stock - previous_stock
                    change_percent = (stock_change / previous_stock * 100) if previous_stock != 0 else 0
                    
                    print(f"{current_date:<12} {current_stock:<8} {stock_change:>+8} {change_percent:>+9.1f}%")
                else:
                    print(f"{current_date:<12} {current_stock:<8} {'N/A':<8} {'N/A':<10}")
                
                previous_stock = current_stock
                previous_date = current_date
        
        elif results:
            print(f"📊 Only one record found for {product_id} on {results[0][0]}: {results[0][1]}")
        else:
            print(f"📭 No stock history found for {product_id}")
    
    finally:
        cursor.close()

def validate_stock_consistency(connection, product_id):
    """
    Kiểm tra tính nhất quán của dữ liệu stock
    """
    cursor = connection.cursor()
    
    try:
        # Lấy stock hiện tại từ product table
        cursor.execute("SELECT stock_quantity, date FROM product WHERE product_id = %s", (product_id,))
        current_product = cursor.fetchone()
        
        if not current_product:
            print(f"❌ Product {product_id} not found")
            return False
        
        current_stock, product_date = current_product
        
        # Lấy stock từ stock_history record mới nhất
        cursor.execute("""
            SELECT stock_quantity, date 
            FROM stock_history 
            WHERE product_id = %s 
            ORDER BY date DESC LIMIT 1
        """, (product_id,))
        
        latest_history = cursor.fetchone()
        
        if latest_history:
            history_stock, history_date = latest_history
            
            if current_stock == history_stock:
                print(f"✅ Stock consistency OK for {product_id}: {current_stock} (as of {history_date})")
                return True
            else:
                print(f"⚠️ Stock inconsistency for {product_id}:")
                print(f"   Product table: {current_stock} (as of {product_date})")
                print(f"   History table: {history_stock} (as of {history_date})")
                return False
        else:
            print(f"ℹ️ No history record for {product_id}")
            return True
    
    finally:
        cursor.close()

def fetch_historical_data(start_date, end_date, slug_value="", start_page=1, end_page=3):
    """
    Thu thập dữ liệu lịch sử cho một khoảng thời gian
    """
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    while current_date <= end_date:
        print(f"\n📅 Fetching data for date: {current_date.strftime('%Y-%m-%d')}")
        fetch_and_save_products(
            start_page=start_page, 
            end_page=end_page, 
            limit_value=102,
            slug_value=slug_value,
            target_date=current_date
        )
        
        current_date += timedelta(days=1)
        time.sleep(1)

def fix_existing_null_created_at(connection):
    """
    Hàm tiện ích để sửa các giá trị NULL trong created_at của dữ liệu hiện có
    """
    cursor = connection.cursor()
    current_time = datetime.now().strftime("%H:%M:%S")
    
    try:
        # Sửa bảng product
        cursor.execute("SELECT COUNT(*) FROM product WHERE created_at IS NULL")
        null_count = cursor.fetchone()[0]
        
        if null_count > 0:
            print(f"🔧 Fixing {null_count} NULL created_at values in product table...")
            cursor.execute("""
                UPDATE product 
                SET created_at = %s 
                WHERE created_at IS NULL
            """, (current_time,))
            print(f"✅ Fixed {cursor.rowcount} records in product table")
        
        # Sửa bảng updated_at nếu cần
        cursor.execute("SELECT COUNT(*) FROM product WHERE updated_at IS NULL")
        null_updated_count = cursor.fetchone()[0]
        
        if null_updated_count > 0:
            print(f"🔧 Fixing {null_updated_count} NULL updated_at values in product table...")
            cursor.execute("""
                UPDATE product 
                SET updated_at = %s 
                WHERE updated_at IS NULL
            """, (current_time,))
            print(f"✅ Fixed {cursor.rowcount} records in product table")
        
        # Sửa bảng stock_history
        cursor.execute("SELECT COUNT(*) FROM stock_history WHERE created_at IS NULL")
        stock_null_count = cursor.fetchone()[0]
        
        if stock_null_count > 0:
            print(f"🔧 Fixing {stock_null_count} NULL created_at values in stock_history table...")
            cursor.execute("""
                UPDATE stock_history 
                SET created_at = %s 
                WHERE created_at IS NULL
            """, (current_time,))
            print(f"✅ Fixed {cursor.rowcount} records in stock_history table")
        
        # Sửa bảng price_history
        cursor.execute("SELECT COUNT(*) FROM price_history WHERE created_at IS NULL")
        price_null_count = cursor.fetchone()[0]
        
        if price_null_count > 0:
            print(f"🔧 Fixing {price_null_count} NULL created_at values in price_history table...")
            cursor.execute("""
                UPDATE price_history 
                SET created_at = %s 
                WHERE created_at IS NULL
            """, (current_time,))
            print(f"✅ Fixed {cursor.rowcount} records in price_history table")
        
        connection.commit()
        print("🎉 All NULL created_at values have been fixed!")
        
    except Error as e:
        print(f"❌ Error fixing NULL values: {e}")
        connection.rollback()
    finally:
        cursor.close()

# Main execution
if __name__ == "__main__":
    # Tạo kết nối và sửa dữ liệu NULL hiện có
    connection = create_connection()
    if connection:
        print("🔧 Fixing existing NULL created_at values...")
        fix_existing_null_created_at(connection)
        connection.close()
    
    # Thu thập dữ liệu lịch sử cho các ngày trước
    fetch_historical_data(
        start_date="2025-09-05", 
        end_date="2025-09-08", 
        slug_value="your-category-slug",
        start_page=1,
        end_page=3
    )
    
    # Ví dụ sử dụng các utility functions:
    connection = create_connection()
    if connection:
        # Tính toán thay đổi stock hàng ngày
        calculate_daily_stock_changes(connection, "your-product-id", days=7)
        
        # Kiểm tra tính nhất quán dữ liệu
        validate_stock_consistency(connection, "your-product-id")
        
        connection.close()