import mysql.connector
import json
import os
import re
import time
import logging
import schedule
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
import threading
from contextlib import contextmanager
import sys
import hashlib
import gc
import subprocess

load_dotenv()

# Cấu hình logging với UTF-8 encoding
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log', encoding='utf-8'),
        logging.FileHandler('migration2.log', encoding='utf-8'),  # Thêm file log mới
        logging.StreamHandler(sys.stdout)
    ]
)

class MongoToMySQLMigration:
    def __init__(self):
        self.mysql_conn = None
        self.mongo_client = None
        self.mongo_db = None
        self.logger = logging.getLogger(__name__)
        
        # Set console handler encoding to UTF-8
        for handler in self.logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.stream = sys.stdout
                
        self.migration_stats = {
            'total_processed': 0,
            'products_migrated': 0,
            'stock_records': 0,
            'price_records': 0,
            'errors': 0,
            'skipped_duplicates': 0,
            'last_migration': None
        }
        
        # Track processed documents to ensure no duplicates
        self.processed_docs = set()
        self.batch_size = 100  # Giảm batch size
        self.max_memory_usage = 500 * 1024 * 1024  # 500MB limit
        self.gc_frequency = 50  # Garbage collect mỗi 50 batches
        self.batch_counter = 0

    @contextmanager
    def get_mysql_connection(self):
        """Context manager for MySQL connections with proper error handling"""
        conn = None
        try:
            conn = mysql.connector.connect(
                host=os.getenv('MYSQL_HOST', 'localhost'),
                port=int(os.getenv('MYSQL_PORT', '3306')),
                user=os.getenv('MYSQL_USERNAME', 'root'),
                password=os.getenv('MYSQL_PASSWORD', '123456789@'),
                database=os.getenv('MYSQL_DATABASE', 'coffee_db'),
                charset='utf8mb4',
                autocommit=False,
                connect_timeout=60,
                read_timeout=60,
                pool_name='migration_pool',
                pool_size=3,
                pool_reset_session=True,
                use_pure=True, 
                consume_results=True
            )
            
            # Tối ưu hóa MySQL settings - chỉ set các SESSION variables
            cursor = conn.cursor()
            try:
                cursor.execute("SET SESSION wait_timeout = 7200")
                cursor.execute("SET SESSION interactive_timeout = 7200")
                cursor.execute("SET SESSION bulk_insert_buffer_size = 67108864")
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 120")
                cursor.execute("SET SESSION max_allowed_packet = 1073741824")  # 1GB
            except mysql.connector.Error as e:
                if "read-only" not in str(e):
                    self.logger.warning(f"MySQL optimization failed: {e}")
            finally:
                cursor.close()
                
            yield conn
        except Exception as e:
            self.logger.error(f"MySQL connection failed: {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    @contextmanager
    def get_mongodb_connection(self):
        """Context manager for MongoDB connections"""
        client = None
        try:
            mongo_uri = os.getenv('MONGO_URI')
            if not mongo_uri:
                raise ValueError("MONGO_URI not found in environment variables")
            
            client = MongoClient(
                mongo_uri, 
                serverSelectionTimeoutMS=30000,
                maxPoolSize=10,
                minPoolSize=2,
                maxIdleTimeMS=30000,
                socketTimeoutMS=20000
            )
            db = client[os.getenv('MONGO_DATABASE', 'coffee_db')]  # Sửa từ 'db_kf' thành 'coffee_db'
            # Test connection
            client.server_info()
            yield db
        except Exception as e:
            self.logger.error(f"MongoDB connection failed: {e}")
            raise
        finally:
            if client:
                client.close()

    def clean_number(self, value):
        """Clean and validate numeric values with better error handling"""
        if value is None:
            return 0
        
        try:
            if isinstance(value, (int, float)):
                return min(max(0, int(round(value))), 9223372036854775807)
            
            if isinstance(value, str):
                # Remove currency symbols and normalize
                cleaned = re.sub(r'[\sVNDUSD$,€¥]', '', value.replace(',', '.'))
                # Remove Vietnamese dong symbol
                cleaned = cleaned.replace('₫', '')
                
                if not cleaned:
                    return 0
                
                # Extract number using regex
                match = re.search(r'-?\d+\.?\d*', cleaned)
                if match:
                    num = float(match.group())
                    return min(max(0, int(round(num))), 9223372036854775807)
                return 0
        except (ValueError, TypeError, AttributeError):
            pass
        
        return 0
    
    
    def generate_unique_product_id(self, doc):
        """Generate unique product ID using document content hash"""
        # Try to get original ID first
        original_id = doc.get('id') or doc.get('product_id')
        
        if original_id and str(original_id).strip() and str(original_id).lower() not in ['none', 'null', '']:
            base_id = re.sub(r'[^\w\-_.]', '_', str(original_id))[:200]
        else:
            # Generate from document content
            content = f"{doc.get('name', '')}{doc.get('category', '')}{doc.get('price', 0)}"
            hash_part = hashlib.md5(content.encode()).hexdigest()[:8]
            base_id = f"product_{hash_part}"
        
        # Create unique identifier using _id
        doc_id = str(doc.get('_id', ''))
        unique_id = f"{base_id}_{doc_id}"[:255]
        
        return unique_id

    def create_table_structure(self):
        """Create optimized table structure with indices"""
        with self.get_mysql_connection() as conn:
            cursor = conn.cursor()
            
            try:
                # Disable foreign key checks and optimize for bulk insert
                cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
                cursor.execute("SET UNIQUE_CHECKS = 0")
                cursor.execute("SET sql_log_bin = 0")
                
                # Drop existing tables
                tables_to_drop = ['price_history', 'stock_history', 'product']
                for table in tables_to_drop:
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                
                # Create product table with optimized structure
                product_sql = """
                CREATE TABLE `product` (
                    `product_id` VARCHAR(255) PRIMARY KEY,
                    `mongo_id` VARCHAR(255),
                    `original_id` VARCHAR(255),
                    `category` VARCHAR(255),
                    `name` TEXT,
                    `price` BIGINT UNSIGNED DEFAULT 0,
                    `promotion` TEXT,
                    `date` DATETIME,
                    `original_price` BIGINT UNSIGNED DEFAULT 0,
                    `stock_quantity` INT DEFAULT 0,
                    `total_sold` INT DEFAULT 0,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX `idx_mongo_id` (`mongo_id`),
                    INDEX `idx_original_id` (`original_id`),
                    INDEX `idx_category` (`category`),
                    INDEX `idx_price` (`price`),
                    INDEX `idx_stock` (`stock_quantity`),
                    INDEX `idx_date` (`date`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(product_sql)
                
                # Create stock_history table
                stock_history_sql = """
                CREATE TABLE `stock_history` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `product_id` VARCHAR(255) NOT NULL,
                    `date` DATETIME NOT NULL,
                    `stock_increased` INT DEFAULT 0,
                    `stock_decreased` INT DEFAULT 0,
                    `note` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX `idx_product_date` (`product_id`, `date`),
                    INDEX `idx_date` (`date`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(stock_history_sql)
                
                # Create price_history table
                price_history_sql = """
                CREATE TABLE `price_history` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `product_id` VARCHAR(255) NOT NULL,
                    `date` DATETIME NOT NULL,
                    `price` BIGINT UNSIGNED DEFAULT 0,
                    `original_price` BIGINT UNSIGNED DEFAULT 0,
                    `note` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX `idx_product_date` (`product_id`, `date`),
                    INDEX `idx_date` (`date`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                cursor.execute(price_history_sql)
                
                # Create migration_log table for tracking
                migration_log_sql = """
                CREATE TABLE IF NOT EXISTS `migration_log` (
                    `id` INT AUTO_INCREMENT PRIMARY KEY,
                    `migration_date` DATETIME NOT NULL,
                    `total_processed` INT DEFAULT 0,
                    `products_migrated` INT DEFAULT 0,
                    `stock_records` INT DEFAULT 0,
                    `price_records` INT DEFAULT 0,
                    `errors` INT DEFAULT 0,
                    `skipped_duplicates` INT DEFAULT 0,
                    `status` ENUM('SUCCESS', 'FAILED', 'PARTIAL') DEFAULT 'SUCCESS',
                    `notes` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """
                
                try:
                    cursor.execute("ALTER TABLE `migration_log` ADD COLUMN `skipped_duplicates` INT DEFAULT 0")
                except mysql.connector.Error:
               # Cột đã tồn tại, bỏ qua
                    pass
                cursor.execute(migration_log_sql)
                
                # Re-enable checks
                cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
                cursor.execute("SET UNIQUE_CHECKS = 1")
                cursor.execute("SET sql_log_bin = 1")
                
                conn.commit()
                self.logger.info("Database schema created successfully")
                
            except Exception as e:
                conn.rollback()
                self.logger.error(f"Failed to create table structure: {e}")
                raise
            finally:
                cursor.close()

    def migrate_products_batch(self, cursor, batch_docs, id_mapping):
        """Migrate products in batch with improved error handling"""
        product_sql = """
        INSERT INTO `product` 
        (`product_id`, `mongo_id`, `original_id`, `category`, `name`, `price`, `promotion`, `date`, `original_price`, `stock_quantity`, `total_sold`) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        `category` = VALUES(`category`),
        `name` = VALUES(`name`),
        `price` = VALUES(`price`),
        `promotion` = VALUES(`promotion`),
        `date` = VALUES(`date`),
        `original_price` = VALUES(`original_price`),
        `stock_quantity` = VALUES(`stock_quantity`),
        `total_sold` = VALUES(`total_sold`),
        `updated_at` = CURRENT_TIMESTAMP
        """
        
        batch_data = []
        successful_products = 0
        for doc in batch_docs:
            try:
                mongo_id = str(doc.get('_id', ''))
                # Tạo product_id duy nhất cho mỗi doc, lưu vào id_mapping
                product_id = self.generate_unique_product_id(doc)
                id_mapping[mongo_id] = product_id
                original_id = doc.get('id') or doc.get('product_id')
                # Parse date safely
                doc_date = doc.get('date')
                if isinstance(doc_date, str):
                    try:
                        doc_date = datetime.strptime(doc_date, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        try:
                            doc_date = datetime.strptime(doc_date, '%Y-%m-%d')
                        except ValueError:
                            doc_date = None  # Giữ nguyên nếu không parse được
                elif not isinstance(doc_date, datetime):
                    doc_date = None
                row = [
                    product_id,
                    mongo_id,
                    str(original_id)[:255] if original_id else None,
                    str(doc.get('category', ''))[:255],
                    str(doc.get('name', ''))[:1000],
                    self.clean_number(doc.get('price', 0)),
                    str(doc.get('promotion', ''))[:1000],
                    doc_date,
                    self.clean_number(doc.get('original_price', 0)),
                    self.clean_number(doc.get('stock_quantity', 0)),
                    self.clean_number(doc.get('total_sold', 0))
                ]
                batch_data.append(row)
                self.processed_docs.add(mongo_id)
                successful_products += 1
            except Exception as e:
                self.logger.error(f"Error processing product document {doc.get('_id')}: {e}")
                self.migration_stats['errors'] += 1
                continue
        if batch_data:
            try:
                cursor.executemany(product_sql, batch_data)
                self.migration_stats['products_migrated'] += successful_products
            except Exception as e:
                self.logger.error(f"Error inserting product batch: {e}")
                for row in batch_data:
                    try:
                        cursor.execute(product_sql, row)
                        self.migration_stats['products_migrated'] += 1
                    except Exception as row_error:
                        self.logger.error(f"Error inserting product {row[0]}: {row_error}")
                        self.migration_stats['errors'] += 1
        return successful_products

    def migrate_stock_history_batch(self, cursor, batch_docs, id_mapping):
        """Migrate stock history in batch"""
        stock_sql = """
        INSERT INTO `stock_history` 
        (`product_id`, `date`, `stock_increased`, `stock_decreased`, `note`) 
        VALUES (%s, %s, %s, %s, %s)
        """
        batch_data = []
        skipped_records = 0
        for doc in batch_docs:
            try:
                mongo_id = str(doc.get('_id', ''))
                if mongo_id not in self.processed_docs or mongo_id not in id_mapping:
                    continue
                product_id = id_mapping[mongo_id]
                stock_history_data = doc.get('stock_history', [])
                if isinstance(stock_history_data, str):
                    try:
                        stock_history_data = json.loads(stock_history_data)
                    except json.JSONDecodeError:
                        stock_history_data = []
                if isinstance(stock_history_data, list) and stock_history_data:
                    for entry in stock_history_data:
                        if isinstance(entry, dict):
                            entry_date = entry.get('date')
                            # Nếu là string thì chuyển sang datetime, còn lại giữ nguyên
                            if isinstance(entry_date, str):
                                try:
                                    entry_date = datetime.strptime(entry_date, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    try:
                                        entry_date = datetime.strptime(entry_date, '%Y-%m-%d')
                                    except ValueError:
                                        entry_date = None
                            row = [
                                product_id,
                                entry_date,
                                self.clean_number(entry.get('stock_increased', entry.get('increased', 0))),
                                self.clean_number(entry.get('stock_decreased', entry.get('decreased', 0))),
                                str(entry.get('note', ''))[:1000]
                            ]
                            batch_data.append(row)
                # Không insert bản ghi nếu không có lịch sử tồn kho
            except Exception as e:
                self.logger.error(f"Error processing stock history for {doc.get('_id')}: {e}")
                self.migration_stats['errors'] += 1
                continue
        if batch_data:
            try:
                cursor.executemany(stock_sql, batch_data)
                self.migration_stats['stock_records'] += len(batch_data)
            except Exception as e:
                self.logger.error(f"Error inserting stock batch: {e}")
                for row in batch_data:
                    try:
                        cursor.execute(stock_sql, row)
                        self.migration_stats['stock_records'] += 1
                    except Exception as row_error:
                        self.logger.error(f"Error inserting stock record: {row_error}")
                        self.migration_stats['errors'] += 1
        if skipped_records > 0:
            self.logger.info(f"Batch skipped {skipped_records} stock_history records due to invalid date format/type.")
        return len(batch_data)

    def migrate_price_history_batch(self, cursor, batch_docs, id_mapping):
        """Migrate price history in batch"""
        price_sql = """
        INSERT INTO `price_history` 
        (`product_id`, `date`, `price`, `original_price`, `note`) 
        VALUES (%s, %s, %s, %s, %s)
        """
        batch_data = []
        skipped_records = 0
        for doc in batch_docs:
            try:
                mongo_id = str(doc.get('_id', ''))
                if mongo_id not in self.processed_docs or mongo_id not in id_mapping:
                    continue
                product_id = id_mapping[mongo_id]
                price_history_data = doc.get('price_history', [])
                if isinstance(price_history_data, str):
                    try:
                        price_history_data = json.loads(price_history_data)
                    except json.JSONDecodeError:
                        price_history_data = []
                if isinstance(price_history_data, list) and price_history_data:
                    for entry in price_history_data:
                        if isinstance(entry, dict):
                            entry_date = entry.get('date') or doc.get('date')
                            # Nếu là string thì chuyển sang datetime, còn lại giữ nguyên
                            if isinstance(entry_date, str):
                                try:
                                    entry_date = datetime.strptime(entry_date, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    try:
                                        entry_date = datetime.strptime(entry_date, '%Y-%m-%d')
                                    except ValueError:
                                        entry_date = None
                            # Đảm bảo entry_date là kiểu datetime
                            row = [
                                product_id,
                                entry_date,
                                self.clean_number(entry.get('price', 0)),
                                self.clean_number(entry.get('original_price', 0)),
                                str(entry.get('note', ''))[:1000]
                            ]
                            batch_data.append(row)
                # Không insert bản ghi nếu không có lịch sử giá
            except Exception as e:
                self.logger.error(f"Error processing price history for {doc.get('_id')}: {e}")
                self.migration_stats['errors'] += 1
                continue
        if batch_data:
            try:
                cursor.executemany(price_sql, batch_data)
                self.migration_stats['price_records'] += len(batch_data)
            except Exception as e:
                self.logger.error(f"Error inserting price batch: {e}")
                for row in batch_data:
                    try:
                        cursor.execute(price_sql, row)
                        self.migration_stats['price_records'] += 1
                    except Exception as row_error:
                        self.logger.error(f"Error inserting price record: {row_error}")
                        self.migration_stats['errors'] += 1
        if skipped_records > 0:
            self.logger.info(f"Batch skipped {skipped_records} price_history records due to invalid date format/type.")
        return len(batch_data)

    def migrate_data(self, batch_size=200):
        """Main migration method with improved batch processing"""
        try:
            with self.get_mongodb_connection() as mongo_db:
                with self.get_mysql_connection() as mysql_conn:
                    collection = mongo_db['kf_new']
                    total_docs = collection.count_documents({})
                    print(f"Total MongoDB documents: {total_docs}")
                    if total_docs == 0:
                        self.logger.warning("No documents found in kf_new collection")
                        return
                    self.logger.info(f"Found {total_docs} documents to migrate")
                    docs_with_errors = 0
                    cursor = None
                    try:
                        cursor = mysql_conn.cursor()
                        self.migration_stats = {
                            'total_processed': 0,
                            'products_migrated': 0,
                            'stock_records': 0,
                            'price_records': 0,
                            'errors': 0,
                            'skipped_duplicates': 0,
                            'last_migration': datetime.now()
                        }
                        self.processed_docs.clear()
                        for start in range(0, total_docs, batch_size):
                            try:
                                batch_docs = list(collection.find().skip(start).limit(batch_size))
                                if not batch_docs:
                                    break
                                id_mapping = {}
                                products_migrated = self.migrate_products_batch(cursor, batch_docs, id_mapping)
                                stock_migrated = self.migrate_stock_history_batch(cursor, batch_docs, id_mapping)
                                price_migrated = self.migrate_price_history_batch(cursor, batch_docs, id_mapping)
                                mysql_conn.commit()
                                self.migration_stats['total_processed'] += len(batch_docs)
                                progress = (start + len(batch_docs)) / total_docs * 100
                                self.logger.info(f"Progress: {progress:.1f}% | "
                                               f"Batch: {start+1}-{start+len(batch_docs)} | "
                                               f"Products: {products_migrated}, "
                                               f"Stock: {stock_migrated}, "
                                               f"Price: {price_migrated} | "
                                               f"Errors: {self.migration_stats['errors']}")
                                # Tối ưu RAM: gọi gc.collect() sau mỗi batch
                                if (start // batch_size) % self.gc_frequency == 0:
                                    gc.collect()
                            except Exception as batch_error:
                                self.logger.error(f"Error processing batch {start}-{start+batch_size}: {batch_error}")
                                mysql_conn.rollback()
                                continue
                        # Final verification
                        cursor.execute("SELECT COUNT(*) FROM product")
                        result = cursor.fetchone()
                        if result is not None:
                            (final_count,) = result
                        else:
                            final_count = 0
                        self.logger.info(f"Final verification: {final_count} products in MySQL vs {total_docs} in MongoDB")
                        self.log_migration_completion(cursor)
                        mysql_conn.commit()
                        self.logger.info("Migration completed successfully")
                        self.logger.info(f"Final stats: {self.migration_stats}")
                    except Exception as e:
                        if mysql_conn:
                            mysql_conn.rollback()
                        self.logger.error(f"Migration failed: {e}")
                        raise
                    finally:
                        if cursor:
                            cursor.close()
        except Exception as e:
            self.logger.error(f"Critical migration error: {e}")
            raise

    def log_migration_completion(self, cursor):
        """Log migration completion to database"""
        log_sql = """
        INSERT INTO migration_log 
        (migration_date, total_processed, products_migrated, stock_records, price_records, errors, skipped_duplicates, status, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        status = 'SUCCESS' if self.migration_stats['errors'] == 0 else 'PARTIAL'
        notes = f"Migration completed with {self.migration_stats['errors']} errors and {self.migration_stats['skipped_duplicates']} duplicates skipped"
        
        cursor.execute(log_sql, [
            self.migration_stats['last_migration'],
            self.migration_stats['total_processed'],
            self.migration_stats['products_migrated'],
            self.migration_stats['stock_records'],
            self.migration_stats['price_records'],
            self.migration_stats['errors'],
            self.migration_stats['skipped_duplicates'],
            status,
            notes
        ])

    def run_migration(self, batch_size=200):
        self.logger.info("Migration triggered by scheduler")  # Log mỗi lần scheduler gọi
        start_time = time.time()
        try:
            self.logger.info("Starting automated migration process...")
            self.create_table_structure()
            self.migrate_data(batch_size=batch_size)
            end_time = time.time()
            duration = end_time - start_time
            self.logger.info(f"Migration completed in {duration:.2f} seconds")
            
            # Tự động chạy dashboard sau khi migration hoàn thành
            self.start_dashboard()
            
            return True
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            return False

    def start_dashboard(self):
        """Tự động chạy dashboard Streamlit"""
        try:
            self.logger.info("Starting Streamlit dashboard...")
            # Chạy dashboard trong background
            dashboard_process = subprocess.Popen([
                sys.executable, "-m", "streamlit", "run", "dashboard.py",
                "--server.port", "8501",
                "--server.headless", "true"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            self.logger.info(f"Dashboard started with PID: {dashboard_process.pid}")
            self.logger.info("Dashboard available at: http://localhost:8501")
            
            # Lưu process ID để có thể dừng sau này nếu cần
            with open('dashboard_pid.txt', 'w') as f:
                f.write(str(dashboard_process.pid))
                
        except Exception as e:
            self.logger.error(f"Failed to start dashboard: {e}")

    def stop_dashboard(self):
        """Dừng dashboard nếu đang chạy"""
        try:
            if os.path.exists('dashboard_pid.txt'):
                with open('dashboard_pid.txt', 'r') as f:
                    pid = f.read().strip()
                if pid:
                    os.kill(int(pid), 15)  # SIGTERM
                    self.logger.info(f"Dashboard stopped (PID: {pid})")
                os.remove('dashboard_pid.txt')
        except Exception as e:
            self.logger.error(f"Failed to stop dashboard: {e}")

    def schedule_migration(self):
        """Schedule automatic migration every 30 minutes"""
        schedule.every(30).minutes.do(self.run_migration)
        self.logger.info("Migration scheduled: every 30 minutes")
        
        # Chạy migration ngay lần đầu
        self.logger.info("Running initial migration...")
        self.run_migration()
        
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

def main():
    """Main function with command line options"""
    import argparse
    parser = argparse.ArgumentParser(description='MongoDB to MySQL Migration Tool')
    parser.add_argument('--mode', choices=['once', 'schedule'], default='once',
                      help='Run migration once or schedule it')
    parser.add_argument('--batch-size', type=int, default=200,
                      help='Batch size for migration (default: 200)')
    args = parser.parse_args()
    migration = MongoToMySQLMigration()
    try:
        if args.mode == 'once':
            migration.run_migration(batch_size=args.batch_size)
        else:
            migration.schedule_migration()
    except KeyboardInterrupt:
        logging.info("Migration interrupted by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()