import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import logging
import hashlib
import json
import decimal
import re
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database connection configurations
def get_db_connection():
    """Kết nối đến MySQL database"""
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='kfm',
            user='root',
            password='123456789@',
            port=3306,
            autocommit=True,
            use_unicode=True,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci',
            connection_timeout=10
        )
        if connection.is_connected():
            logger.info("Kết nối MySQL thành công")
            return connection
        else:
            logger.error("MySQL connection failed")
            return None
    except mysql.connector.Error as e:
        logger.error(f"MySQL Error: {e}")
        return None
    except Exception as e:
        logger.error(f"Lỗi kết nối MySQL: {e}")
        return None
def fetch_revenue_by_category_time(view_type='day', start_date=None, end_date=None):
    """Lấy doanh thu theo danh mục và thời gian cho stacked bar chart"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            if not start_date:
                start_date = datetime(2025, 9, 5).date()
            if not end_date:
                end_date = datetime.now()
            
            # Truy vấn dữ liệu theo view_type
            if view_type == 'day':
                query = """
                SELECT DATE(sh.date) as period, p.category,
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s AND DATE(sh.date) <= %s
                GROUP BY DATE(sh.date), p.category
                ORDER BY DATE(sh.date), p.category
                """
            elif view_type == 'week':
                query = """
                SELECT CONCAT(YEAR(sh.date), '-W', LPAD(WEEK(sh.date), 2, '0')) as period, p.category,
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s AND DATE(sh.date) <= %s
                GROUP BY YEAR(sh.date), WEEK(sh.date), p.category
                ORDER BY YEAR(sh.date), WEEK(sh.date), p.category
                """
            elif view_type == 'month':
                query = """
                SELECT DATE_FORMAT(sh.date, '%Y-%m') as period, p.category,
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s AND DATE(sh.date) <= %s
                GROUP BY DATE_FORMAT(sh.date, '%Y-%m'), p.category
                ORDER BY DATE_FORMAT(sh.date, '%Y-%m'), p.category
                """
            else:  # year
                query = """
                SELECT YEAR(sh.date) as period, p.category,
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s AND DATE(sh.date) <= %s
                GROUP BY YEAR(sh.date), p.category
                ORDER BY YEAR(sh.date), p.category
                """
            
            cursor.execute(query, [start_date, end_date])
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=['period', 'category', 'revenue'])
                return df
            else:
                return pd.DataFrame(columns=['period', 'category', 'revenue'])
                
        except Exception as e:
            logger.error(f"Lỗi truy vấn doanh thu theo danh mục và thời gian: {e}")
            return pd.DataFrame(columns=['period', 'category', 'revenue'])
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=['period', 'category', 'revenue'])

def main():
    st.set_page_config(
        page_title="☕ Coffee Shop Dashboard",
        page_icon="☕",
        layout="wide"
    )
    
    st.title("☕ Coffee Shop Dashboard")
    st.markdown("Dashboard quản lý cửa hàng cà phê")
    
    # Enhanced Stacked Bar Chart for Revenue by Category over Time
    st.header("📊 Doanh thu theo danh mục theo thời gian")
    
    # Time period selector
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        start_date = st.date_input("Từ ngày:", datetime(2025, 9, 5).date())
    with col2:
        end_date = st.date_input("Đến ngày:", datetime.now())
    with col3:
        view_type = st.selectbox("Xem theo:", ['day', 'week', 'month', 'year'], 
                               format_func=lambda x: {'day': 'Ngày', 'week': 'Tuần', 'month': 'Tháng', 'year': 'Năm'}[x])
    
    # Fetch stacked data
    stack_df = fetch_revenue_by_category_time(view_type, start_date, end_date)
    
    if not stack_df.empty:
        # Create stacked bar chart
        # Create title based on view_type
        title_map = {'day': 'ngày', 'week': 'tuần', 'month': 'tháng', 'year': 'năm'}
        chart_title = f"Doanh thu theo danh mục theo {title_map[view_type]}"
        
        fig_stack = px.bar(
            stack_df,
            x='period',
            y='revenue',
            color='category',
            title=chart_title,
            labels={'revenue': 'Doanh thu (₫)', 'period': 'Thời gian', 'category': 'Danh mục'},
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        # Create x-axis title based on view_type
        xaxis_title_map = {'day': 'Ngày', 'week': 'Tuần', 'month': 'Tháng', 'year': 'Năm'}
        xaxis_title = f"Thời gian ({xaxis_title_map[view_type]})"
        
        fig_stack.update_layout(
            height=500,
            xaxis_title=xaxis_title,
            yaxis_title="Doanh thu (₫)",
            font=dict(size=12),
            title_font_size=16,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        # Rotate x-axis labels if needed
        if view_type == 'day':
            fig_stack.update_xaxes(tickangle=45)
        
        st.plotly_chart(fig_stack, use_container_width=True)
        
        # Show highest revenue period
        period_totals = stack_df.groupby('period')['revenue'].sum().reset_index()
        if not period_totals.empty:
            max_period = period_totals.loc[period_totals['revenue'].idxmax()]
            period_type_map = {'day': 'Ngày', 'week': 'Tuần', 'month': 'Tháng', 'year': 'Năm'}
            st.success(f"🏆 **{period_type_map[view_type]} bán cao nhất**: {max_period['period']} với doanh thu {max_period['revenue']:,.0f}₫")
    else:
        st.info("📊 Không có dữ liệu doanh thu trong khoảng thời gian đã chọn")

# Database watcher class
class DatabaseWatcher:
    def __init__(self):
        self.last_hash = None
        self.observers = []
    
    def get_database_hash(self):
        """Tạo hash từ dữ liệu database để detect thay đổi"""
        try:
            connection = get_db_connection()
            if connection:
                cursor = connection.cursor()
                
                # Get hash of key tables
                cursor.execute("SELECT COUNT(*), COALESCE(MAX(UNIX_TIMESTAMP(updated_at)), UNIX_TIMESTAMP(NOW())) FROM product")
                product_data = cursor.fetchone()
                
                cursor.execute("SELECT COUNT(*), COALESCE(MAX(UNIX_TIMESTAMP(date)), UNIX_TIMESTAMP(NOW())) FROM stock_history")
                stock_data = cursor.fetchone()
                
                # Create hash
                data_string = f"{product_data}{stock_data}"
                return hashlib.md5(data_string.encode()).hexdigest()
        except Exception as e:
            logger.error(f"Lỗi tạo hash database: {e}")
            return None
    
    def check_for_changes(self):
        """Check for database changes"""
        current_hash = self.get_database_hash()
        if current_hash and current_hash != self.last_hash:
            self.last_hash = current_hash
            return True
        return False

# Enhanced data fetching functions
def fetch_slow_sellers(start_date=None, end_date=None, limit=10, category_filter=None, search_keyword=None):
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            query = """
            SELECT p.name, COUNT(sh.id) as transaction_count
            FROM product p
            LEFT JOIN stock_history sh ON p.product_id = sh.product_id AND sh.stock_decreased > 0
            WHERE 1=1
            """
            params = []
            if start_date:
                query += " AND (sh.date IS NULL OR DATE(sh.date) >= %s)"
                params.append(start_date)
            if end_date:
                query += " AND (sh.date IS NULL OR DATE(sh.date) <= %s)"
                params.append(end_date)
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            query += " GROUP BY p.product_id, p.name HAVING transaction_count >= 1 AND transaction_count <= 10 ORDER BY transaction_count ASC"
            cursor.execute(query, params)
            results = cursor.fetchall()
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['product_name', 'transaction_count']))
                # Ưu tiên mỗi mức transaction_count một sản phẩm
                diverse_df = df.drop_duplicates(subset=['transaction_count'], keep='first')
                # Nếu chưa đủ 10, bổ sung thêm các sản phẩm còn lại
                if len(diverse_df) < limit:
                    extra = df[~df.index.isin(diverse_df.index)].head(limit - len(diverse_df))
                    diverse_df = pd.concat([diverse_df, extra])
                return diverse_df.head(limit)
            else:
                return pd.DataFrame(columns=pd.Index(['product_name', 'transaction_count']))
        except Exception as e:
            logger.error(f'Lỗi truy vấn sản phẩm bán chậm: {e}')
            return pd.DataFrame(columns=pd.Index(['product_name', 'transaction_count']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['product_name', 'transaction_count']))

@st.cache_data(ttl=60)
def fetch_inventory_status():
    """Lấy trạng thái tồn kho"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT 
                p.name,
                p.stock_quantity,
                CASE 
                    WHEN p.stock_quantity = 0 THEN 'Hết hàng'
                    WHEN p.stock_quantity <= 10 THEN 'Sắp hết'
                    WHEN p.stock_quantity <= 50 THEN 'Tồn kho thấp'
                    ELSE 'Tồn kho tốt'
                END as status,
                p.price,
                p.category
            FROM product p
            ORDER BY p.stock_quantity ASC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['product_name', 'stock_quantity', 'status', 'price', 'category']))
                return df
            else:
                return pd.DataFrame(columns=pd.Index(['product_name', 'stock_quantity', 'status', 'price', 'category']))
        except Exception as e:
            logger.error(f"Lỗi truy vấn trạng thái tồn kho: {e}")
            return pd.DataFrame(columns=pd.Index(['product_name', 'stock_quantity', 'status', 'price', 'category']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['product_name', 'stock_quantity', 'status', 'price', 'category']))

def fetch_all_products(category_filter=None, search_keyword=None):
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            query = """
            SELECT 
                product_id,
                name,
                category,
                price,
                stock_quantity,
                promotion,
                date
            FROM product
            WHERE 1=1
            """
            params = []
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            query += " ORDER BY name"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 'Khuyến mãi', 'Ngày'
                ]))
                return df
            else:
                return pd.DataFrame(columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 'Khuyến mãi', 'Ngày'
                ]))
        except Exception as e:
            logger.error(f"Lỗi truy vấn tất cả sản phẩm: {e}")
            return pd.DataFrame(columns=pd.Index([
                'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 'Khuyến mãi', 'Ngày'
            ]))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index([
        'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 'Khuyến mãi', 'Ngày'
    ]))

def fetch_all_stock_history(start_date=None, end_date=None, category_filter=None, search_keyword=None):
    """Lấy tất cả lịch sử tồn kho, có thể lọc theo khoảng thời gian"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            query = """
            SELECT 
                sh.id,
                p.name as product_name,
                sh.stock_increased,
                sh.stock_decreased,
                sh.date
            FROM stock_history sh
            JOIN product p ON sh.product_id = p.product_id
            WHERE 1=1
            """
            params = []
            
            if start_date:
                query += " AND DATE(sh.date) >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND DATE(sh.date) <= %s"
                params.append(end_date)
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
                
            query += " ORDER BY sh.date DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Nhập kho', 'Xuất kho', 'Ngày'
                ]))
                return df
            else:
                return pd.DataFrame(columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Nhập kho', 'Xuất kho', 'Ngày'
                ]))
        except Exception as e:
            logger.error(f"Lỗi truy vấn lịch sử tồn kho: {e}")
            return pd.DataFrame(columns=pd.Index([
                'ID', 'Tên sản phẩm', 'Nhập kho', 'Xuất kho', 'Ngày'
            ]))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index([
        'ID', 'Tên sản phẩm', 'Nhập kho', 'Xuất kho', 'Ngày'
    ]))

def fetch_all_price_history(start_date=None, end_date=None, category_filter=None, search_keyword=None):
    """Lấy tất cả lịch sử giá, có thể lọc theo khoảng thời gian"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            query = """
            SELECT 
                ph.id,
                p.name as product_name,
                ph.price,
                ph.original_price,
                ph.date
            FROM price_history ph
            JOIN product p ON ph.product_id = p.product_id
            WHERE 1=1
            """
            params = []
            
            if start_date:
                query += " AND DATE(ph.date) >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND DATE(ph.date) <= %s"
                params.append(end_date)
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
                
            query += " ORDER BY ph.date DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Giá mới', 'Giá cũ', 'Ngày'
                ]))
                return df
            else:
                return pd.DataFrame(columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Giá mới', 'Giá cũ', 'Ngày'
                ]))
        except Exception as e:
            logger.error(f"Lỗi truy vấn lịch sử giá: {e}")
            return pd.DataFrame(columns=pd.Index([
                'ID', 'Tên sản phẩm', 'Giá mới', 'Giá cũ', 'Ngày'
            ]))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index([
        'ID', 'Tên sản phẩm', 'Giá mới', 'Giá cũ', 'Ngày'
    ]))

def fetch_sales_summary():
    """Lấy tổng hợp bán hàng theo sản phẩm"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            query = """
            SELECT 
                p.product_id,
                p.name,
                p.category,
                p.price,
                p.stock_quantity,
                COALESCE(SUM(sh.stock_decreased), 0) as total_sold,
                COALESCE(SUM(sh.stock_decreased * p.price), 0) as total_revenue,
                COUNT(DISTINCT DATE(sh.date)) as days_sold
            FROM product p
            LEFT JOIN stock_history sh ON p.product_id = sh.product_id AND sh.stock_decreased > 0
            GROUP BY p.product_id, p.name, p.category, p.price, p.stock_quantity
            ORDER BY total_revenue DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 
                    'Đã bán', 'Doanh thu', 'Số ngày bán'
                ]))
                return df
            else:
                return pd.DataFrame(columns=pd.Index([
                    'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 
                    'Đã bán', 'Doanh thu', 'Số ngày bán'
                ]))
        except Exception as e:
            logger.error(f"Lỗi truy vấn tổng hợp bán hàng: {e}")
            return pd.DataFrame(columns=pd.Index([
                'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 
                'Đã bán', 'Doanh thu', 'Số ngày bán'
            ]))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index([
        'ID', 'Tên sản phẩm', 'Danh mục', 'Giá', 'Tồn kho', 
        'Đã bán', 'Doanh thu', 'Số ngày bán'
    ]))
    
# Original functions (keeping all existing functions)
def fetch_total_products():
    """Lấy tổng số sản phẩm"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM product")
            result = cursor.fetchone()
            if result is not None:
                (product_count,) = result
            else:
                product_count = 0
            
            # Log để debug
            logger.info(f"Tổng số sản phẩm trong MySQL: {product_count}")
            
            return product_count
        except Exception as e:
            logger.error(f"Lỗi truy vấn sản phẩm: {e}")
            return 0
        finally:
            if connection.is_connected():
                connection.close()
    return 0

def fetch_total_revenue(start_date=None, end_date=None, category=None, price_range=None, search_keyword=None):
    """Lấy tổng doanh thu theo điều kiện"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT COALESCE(SUM(sh.stock_decreased * p.price), 0) as total_revenue
            FROM stock_history sh
            JOIN product p ON sh.product_id = p.product_id
            WHERE sh.stock_decreased > 0
            """
            params = []
            
            if start_date:
                query += " AND DATE(sh.date) >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND DATE(sh.date) <= %s"
                params.append(end_date)
            
            if category and category != 'all':
                query += " AND p.category = %s"
                params.append(category)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            if price_range and price_range != 'all':
                if price_range == 'high':
                    query += " AND p.price > 75000"
                elif price_range == 'medium':
                    query += " AND p.price BETWEEN 31500 AND 75000"
                elif price_range == 'low':
                    query += " AND p.price < 31500"
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result is not None:
                (total_revenue,) = result
                return total_revenue
            else:
                return 0
        except Exception as e:
            logger.error(f"Lỗi truy vấn doanh thu: {e}")
            return 0
        finally:
            if connection.is_connected():
                connection.close()
    return 0

def fetch_total_stock():
    """Lấy tổng tồn kho hiện tại"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT COALESCE(SUM(stock_quantity), 0) FROM product")
            result = cursor.fetchone()
            if result is not None:
                (total_stock,) = result
                return total_stock
            else:
                return 0
        except Exception as e:
            logger.error(f"Lỗi truy vấn tồn kho: {e}")
            return 0
        finally:
            if connection.is_connected():
                connection.close()
    return 0

def fetch_total_sold(start_date=None, end_date=None, category_filter=None, search_keyword=None):
    """Lấy tổng số sản phẩm đã bán"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT COALESCE(SUM(sh.stock_decreased), 0) as total_sold
            FROM stock_history sh
            JOIN product p ON sh.product_id = p.product_id
            WHERE sh.stock_decreased > 0
            """
            params = []
            
            if start_date:
                query += " AND DATE(sh.date) >= %s"
                params.append(start_date)
            
            if end_date:
                query += " AND DATE(sh.date) <= %s"
                params.append(end_date)
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            if result is not None:
                (total_sold,) = result
                return total_sold
            else:
                return 0
        except Exception as e:
            logger.error(f"Lỗi truy vấn số lượng bán: {e}")
            return 0
        finally:
            if connection.is_connected():
                connection.close()
    return 0

def fetch_best_worst_sellers(start_date=None, end_date=None, limit=10, category_filter=None, search_keyword=None):
    """Lấy sản phẩm bán chạy nhất (bao gồm cả sản phẩm chưa bán)"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT p.name, COALESCE(SUM(sh.stock_decreased), 0) as total_sold
            FROM product p
            LEFT JOIN stock_history sh ON p.product_id = sh.product_id AND sh.stock_decreased > 0
            WHERE 1=1
            """
            params = []
            
            if start_date:
                query += " AND (sh.date IS NULL OR DATE(sh.date) >= %s)"
                params.append(start_date)
            
            if end_date:
                query += " AND (sh.date IS NULL OR DATE(sh.date) <= %s)"
                params.append(end_date)
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            # Bỏ điều kiện HAVING total_sold > 0 để hiển thị cả sản phẩm chưa bán
            query += " GROUP BY p.product_id, p.name ORDER BY total_sold DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['product_name', 'total_sold']))
                return df
            else:
                return pd.DataFrame(columns=pd.Index(['product_name', 'total_sold']))
        except Exception as e:
            logger.error(f"Lỗi truy vấn sản phẩm bán chạy: {e}")
            return pd.DataFrame(columns=pd.Index(['product_name', 'total_sold']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['product_name', 'total_sold']))

def fetch_sales_trend(view_type='day', start_date=None, end_date=None, category_filter=None, search_keyword=None):
    """Lấy xu hướng bán hàng theo ngày/tháng/năm với chuỗi thời gian liên tục"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            # Thiết lập ngày mặc định nếu không có
            if not start_date:
                start_date = datetime(2025, 9, 5).date()
            if not end_date:
                end_date = datetime.now()
            
            # Truy vấn dữ liệu thực tế
            if view_type == 'day':
                query = """
                SELECT DATE(sh.date) as period, 
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s
                AND DATE(sh.date) <= %s
                """
            elif view_type == 'month':
                query = """
                SELECT DATE_FORMAT(sh.date, '%Y-%m') as period, 
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s
                AND DATE(sh.date) <= %s
                """
            else:  # year
                query = """
                SELECT YEAR(sh.date) as period, 
                       COALESCE(SUM(sh.stock_decreased * p.price), 0) as revenue
                FROM stock_history sh
                JOIN product p ON sh.product_id = p.product_id
                WHERE sh.stock_decreased > 0
                AND DATE(sh.date) >= %s
                AND DATE(sh.date) <= %s
                """
            
            params = [start_date, end_date]
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            if view_type == 'day':
                query += " GROUP BY DATE(sh.date) ORDER BY DATE(sh.date)"
            elif view_type == 'month':
                query += " GROUP BY DATE_FORMAT(sh.date, '%Y-%m') ORDER BY DATE_FORMAT(sh.date, '%Y-%m')"
            else:
                query += " GROUP BY YEAR(sh.date) ORDER BY YEAR(sh.date)"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            # Tạo DataFrame từ kết quả truy vấn
            if results:
                sales_df = pd.DataFrame(list(results), columns=pd.Index(['period', 'revenue']))
            else:
                sales_df = pd.DataFrame(columns=pd.Index(['period', 'revenue']))
            
            # Tạo chuỗi thời gian liên tục
            if view_type == 'day':
                date_range = pd.date_range(start=start_date, end=end_date, freq='D')
                full_range = pd.DataFrame({'period': date_range.strftime('%Y-%m-%d')})
            elif view_type == 'month':
                date_range = pd.date_range(start=start_date, end=end_date, freq='M')
                full_range = pd.DataFrame({'period': date_range.strftime('%Y-%m')})
            else:  # year
                date_range = pd.date_range(start=start_date, end=end_date, freq='Y')
                full_range = pd.DataFrame({'period': date_range.strftime('%Y')})
            
            # Merge với dữ liệu thực tế
            if not sales_df.empty:
                # Chuyển đổi period thành string để merge
                sales_df['period'] = sales_df['period'].astype(str)
                full_range['period'] = full_range['period'].astype(str)
                
                # Merge và fill revenue = 0 cho những ngày không có dữ liệu
                result_df = pd.merge(full_range, sales_df, on='period', how='left')
                result_df['revenue'] = result_df['revenue'].fillna(0)
            else:
                result_df = full_range
                result_df['revenue'] = 0
            
            return result_df
            
        except Exception as e:
            logger.error(f"Lỗi truy vấn xu hướng: {e}")
            return pd.DataFrame(columns=pd.Index(['period', 'revenue']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['period', 'revenue']))

def fetch_price_segments_kmeans(category_filter=None, search_keyword=None):
    """Lấy phân khúc giá sử dụng thuật toán K-Means"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT p.price, p.name, p.stock_quantity,
                   COALESCE(SUM(sh.stock_decreased), 0) as total_sold
            FROM product p
            LEFT JOIN stock_history sh ON p.product_id = sh.product_id AND sh.stock_decreased > 0
            WHERE p.price > 0
            """
            
            params = []
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            query += " GROUP BY p.product_id, p.price, p.name, p.stock_quantity"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results and len(results) >= 3:
                df = pd.DataFrame(list(results), columns=['price', 'name', 'stock_quantity', 'total_sold'])
                
                # Prepare data for K-Means (price and stock_quantity)
                X = df[['price', 'stock_quantity']].values
                scaler = StandardScaler()
                X_scaled = scaler.fit_transform(X)
                
                # Apply K-Means clustering with 3 clusters
                kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
                clusters = kmeans.fit_predict(X_scaled)
                
                # Add cluster labels to dataframe
                df['cluster'] = clusters
                
                # Determine cluster names based on average price
                cluster_stats = df.groupby('cluster')['price'].mean().sort_values()
                cluster_names = {}
                for i, cluster_id in enumerate(cluster_stats.index):
                    if i == 0:
                        cluster_names[cluster_id] = 'Giá thấp'
                    elif i == 1:
                        cluster_names[cluster_id] = 'Giá trung'
                    else:
                        cluster_names[cluster_id] = 'Giá cao'
                
                df['price_segment'] = df['cluster'].map(cluster_names)
                
                # Count products and total sold in each segment
                segment_summary = df.groupby('price_segment').agg({
                    'name': 'count',
                    'total_sold': 'sum'
                }).reset_index()
                segment_summary.columns = ['price_segment', 'product_count', 'total_sold']
                
                return segment_summary
            else:
                # Fallback to traditional segmentation if not enough data
                return pd.DataFrame(columns=pd.Index(['price_segment', 'product_count', 'total_sold']))
                
        except Exception as e:
            logger.error(f"Lỗi phân khúc giá: {e}")
            return pd.DataFrame(columns=pd.Index(['price_segment', 'product_count', 'total_sold']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['price_segment', 'product_count', 'total_sold']))

def fetch_categories():
    """Lấy danh sách danh mục"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT DISTINCT category FROM product WHERE category IS NOT NULL ORDER BY category")
            results = cursor.fetchall()
            return [row[0] for row in results if row is not None and isinstance(row, (tuple, list))]
        except Exception as e:
            logger.error(f"Lỗi truy vấn danh mục: {e}")
            return []
        finally:
            if connection.is_connected():
                connection.close()
    return []

def fetch_category_analysis(category_filter=None, search_keyword=None):
    """Lấy dữ liệu phân tích theo danh mục"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT 
                p.category,
                COUNT(p.product_id) as product_count,
                COALESCE(SUM(sh.stock_decreased * p.price), 0) as total_revenue,
                COALESCE(SUM(sh.stock_decreased), 0) as total_sold
            FROM product p
            LEFT JOIN stock_history sh ON p.product_id = sh.product_id AND sh.stock_decreased > 0
            WHERE p.category IS NOT NULL
            """
            
            params = []
            
            if category_filter and category_filter != 'Tất cả':
                query += " AND p.category = %s"
                params.append(category_filter)
            
            if search_keyword:
                query += " AND p.name LIKE %s"
                params.append(f"%{search_keyword}%")
            
            query += " GROUP BY p.category ORDER BY total_revenue DESC"
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['category', 'product_count', 'total_revenue', 'total_sold']))
                return df
            else:
                return pd.DataFrame(columns=pd.Index(['category', 'product_count', 'total_revenue', 'total_sold']))
        except Exception as e:
            logger.error(f"Lỗi truy vấn phân tích danh mục: {e}")
            return pd.DataFrame(columns=pd.Index(['category', 'product_count', 'total_revenue', 'total_sold']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['category', 'product_count', 'total_revenue', 'total_sold']))

def fetch_price_history(product_id=None, limit=20):
    """Lấy lịch sử giá sản phẩm"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            # Sử dụng LEFT JOIN để tránh lỗi khi product_id không khớp
            if product_id:
                query = """
                SELECT ph.date, COALESCE(p.name, CONCAT('Product ', ph.product_id)) as name, 
                       ph.price, ph.original_price
                FROM price_history ph
                LEFT JOIN product p ON ph.product_id = p.product_id
                WHERE ph.product_id = %s
                ORDER BY ph.date DESC
                LIMIT %s
                """
                params = [product_id, limit]
            else:
                query = """
                SELECT ph.date, COALESCE(p.name, CONCAT('Product ', ph.product_id)) as name, 
                       ph.price, ph.original_price
                FROM price_history ph
                LEFT JOIN product p ON ph.product_id = p.product_id
                ORDER BY ph.date DESC
                LIMIT %s
                """
                params = [limit]
            
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['date', 'product_name', 'price', 'original_price']))
                return df
            else:
                return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'price', 'original_price']))
        except Exception as e:
            logger.error(f"Lỗi truy vấn lịch sử giá: {e}")
            return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'price', 'original_price']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'price', 'original_price']))

def fetch_stock_changes(limit=20):
    """Lấy lịch sử thay đổi tồn kho"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            query = """
            SELECT sh.date, p.name, sh.stock_increased, sh.stock_decreased
            FROM stock_history sh
            JOIN product p ON sh.product_id = p.product_id
            WHERE (sh.stock_increased > 0 OR sh.stock_decreased > 0)
            ORDER BY sh.date DESC
            LIMIT %s
            """
            
            cursor.execute(query, [limit])
            results = cursor.fetchall()
            
            if results:
                df = pd.DataFrame(list(results), columns=pd.Index(['date', 'product_name', 'stock_increased', 'stock_decreased']))
                return df
            else:
                return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'stock_increased', 'stock_decreased']))
        except Exception as e:
            logger.error(f"Lỗi truy vấn lịch sử tồn kho: {e}")
            return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'stock_increased', 'stock_decreased']))
        finally:
            if connection.is_connected():
                connection.close()
    return pd.DataFrame(columns=pd.Index(['date', 'product_name', 'stock_increased', 'stock_decreased']))

def test_connection():
    """Test kết nối database và hiển thị thông tin cơ bản"""
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            # Kiểm tra số lượng bảng
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables if table is not None and isinstance(table, (tuple, list))]
            st.success(f"Kết nối MySQL thành công! Database có {len(table_names)} bảng: {table_names}")
            
            # Kiểm tra số lượng sản phẩm
            cursor.execute("SELECT COUNT(*) FROM product")
            result = cursor.fetchone()
            if result is not None:
                (product_count,) = result
            else:
                product_count = 0
            st.info(f"Tổng số sản phẩm: {product_count}")
            
            # Kiểm tra số record trong stock_history
            cursor.execute("SELECT COUNT(*) FROM stock_history")
            result = cursor.fetchone()
            if result is not None:
                (stock_history_count,) = result
            else:
                stock_history_count = 0
            st.info(f"Tổng số bản ghi stock_history: {stock_history_count}")
            
            # Kiểm tra số record trong price_history
            cursor.execute("SELECT COUNT(*) FROM price_history")
            result = cursor.fetchone()
            if result is not None:
                (price_history_count,) = result
            else:
                price_history_count = 0
            st.info(f"Tổng số bản ghi price_history: {price_history_count}")
            
            # Debug: Kiểm tra dữ liệu price_history trong tháng 3
            cursor.execute("""
                SELECT COUNT(*) as total_records,
                       MIN(date) as min_date, 
                       MAX(date) as max_date,
                       COUNT(DISTINCT product_id) as unique_products
                FROM price_history 
                WHERE DATE(date) >= '2025-03-01' AND DATE(date) <= '2025-03-31'
            """)
            march_data = cursor.fetchone()
            if march_data:
                total_records, min_date, max_date, unique_products = march_data
                st.info(f"Tháng 3/2025 - Tổng records: {total_records}, Min date: {min_date}, Max date: {max_date}, Unique products: {unique_products}")
            
            # Debug: Lấy một vài mẫu dữ liệu price_history
            cursor.execute("""
                SELECT ph.date, ph.price, ph.original_price, COALESCE(p.name, CONCAT('Product ', ph.product_id)) as name
                FROM price_history ph
                LEFT JOIN product p ON ph.product_id = p.product_id
                WHERE DATE(ph.date) >= '2025-03-01' AND DATE(ph.date) <= '2025-03-31'
                LIMIT 5
            """)
            sample_data = cursor.fetchall()
            if sample_data:
                st.info("Mẫu dữ liệu price_history tháng 3:")
                for row in sample_data:
                    st.write(f"- {row[3]}: {row[0]} - Giá: {row[1]}, Giá gốc: {row[2]}")
            
        except Exception as e:
            logger.error(f"Lỗi kiểm tra database: {e}")
            st.error(f"Lỗi kiểm tra database: {e}")
        finally:
            if connection.is_connected():
                connection.close()
    else:
        st.error("Không thể kết nối MySQL!")

def get_changed_products_in_period(start_date, end_date):
    connection = get_db_connection()
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            # Debug: Kiểm tra dữ liệu trong price_history
            debug_query = """
            SELECT COUNT(*) as total_records,
                   MIN(date) as min_date, 
                   MAX(date) as max_date,
                   COUNT(DISTINCT product_id) as unique_products
            FROM price_history 
            WHERE DATE(date) >= %s AND DATE(date) <= %s
            """
            cursor.execute(debug_query, [start_date, end_date])
            debug_result = cursor.fetchone()
            logger.info(f"Debug price_history for {start_date} to {end_date}: {debug_result}")
            
            # Lấy sản phẩm thay đổi giá với chi tiết - Sử dụng LEFT JOIN để tránh lỗi
            price_query = """
            SELECT COALESCE(p.name, CONCAT('Product ', ph.product_id)) as name, 
                   COUNT(*) as change_count, 
                   MIN(ph.date) as first_change, MAX(ph.date) as last_change
            FROM price_history ph
            LEFT JOIN product p ON ph.product_id = p.product_id
            WHERE DATE(ph.date) >= %s AND DATE(ph.date) <= %s
            GROUP BY ph.product_id, p.name
            ORDER BY change_count DESC, name
            """
            cursor.execute(price_query, [start_date, end_date])
            price_changes = []
            for row in cursor.fetchall():
                name, count, first_change, last_change = row
                price_changes.append({
                    'name': name,
                    'change_count': count,
                    'first_change': first_change,
                    'last_change': last_change
                })
            
            # Lấy sản phẩm thay đổi tồn kho với chi tiết - Sử dụng LEFT JOIN
            stock_query = """
            SELECT COALESCE(p.name, CONCAT('Product ', sh.product_id)) as name, 
                   COUNT(*) as change_count,
                   SUM(CASE WHEN sh.stock_increased > 0 THEN 1 ELSE 0 END) as import_count,
                   SUM(CASE WHEN sh.stock_decreased > 0 THEN 1 ELSE 0 END) as export_count,
                   MIN(sh.date) as first_change, MAX(sh.date) as last_change
            FROM stock_history sh
            LEFT JOIN product p ON sh.product_id = p.product_id
            WHERE DATE(sh.date) >= %s AND DATE(sh.date) <= %s
            AND (sh.stock_increased > 0 OR sh.stock_decreased > 0)
            GROUP BY sh.product_id, p.name
            ORDER BY change_count DESC, name
            """
            cursor.execute(stock_query, [start_date, end_date])
            stock_changes = []
            for row in cursor.fetchall():
                name, count, import_count, export_count, first_change, last_change = row
                stock_changes.append({
                    'name': name,
                    'change_count': count,
                    'import_count': import_count,
                    'export_count': export_count,
                    'first_change': first_change,
                    'last_change': last_change
                })
            
            return {
                'price_changes': price_changes,
                'stock_changes': stock_changes
            }
        except Exception as e:
            logger.error(f"Lỗi lấy sản phẩm thay đổi: {e}")
            return None
        finally:
            if connection.is_connected():
                connection.close()
    return None

def main():
    st.set_page_config(
        page_title="KingFoodMart Dashboard",
        page_icon="🏪",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    if 'watcher' not in st.session_state:
        st.session_state.watcher = DatabaseWatcher()
    
    if 'last_update' not in st.session_state:
        st.session_state.last_update = datetime.now()
    
    # Custom CSS
    st.markdown("""
    <style>
    .main > div {
        padding-top: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stSelectbox, .stDateInput {
        margin-bottom: 1rem;
    }
    .stAlert {
        margin-bottom: 1rem;
    }
    .auto-refresh {
        background-color: #e8f5e8;
        padding: 0.5rem;
        border-radius: 0.3rem;
        border-left: 4px solid #28a745;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.title("🏪 KingFoodMart Dashboard")
    st.markdown("**Báo cáo doanh số và tồn kho thời gian thực với đồng bộ tự động**")
    
    # Hiển thị thông tin dữ liệu hiện tại
    current_products = fetch_total_products()
    st.info(f"📊 **Dữ liệu hiện tại**: {current_products:,} sản phẩm trong data base")
    
    # Sidebar controls
    st.sidebar.header("🔧 Bộ lọc")

    # Test connection button
    if st.sidebar.button("🔌 Test Database"):
        test_connection()
    
    # Dashboard view type
    dashboard_view = st.sidebar.selectbox(
        "Trạng thái :",
        options=['sales', 'inventory'],
        format_func=lambda x: {'sales': '📊 Bán hàng', 'inventory': '📦 Tồn kho'}[x]
    )
    
    view_type = 'day'

    
    # Date range with unlimited end date option
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "Từ ngày:", 
            value=datetime(2025, 9, 5).date(),
            help="Chọn ngày bắt đầu cho báo cáo"
        )
    
    with col2:
        end_date = st.date_input(
            "Đến ngày:", 
            value=datetime.now(),
            min_value=None,
            max_value=None,
            help="Chọn ngày kết thúc (có thể chọn ngày tương lai)"
        )
    
    # Price range
    price_range = st.sidebar.selectbox(
        "Phân khúc giá:",
        options=['all', 'high', 'medium', 'low'],
        format_func=lambda x: {
            'all': 'Tất cả',
            'high': 'Cao (>75k)',
            'medium': 'Trung (31.5k-75k)',
            'low': 'Thấp (<31.5k)'
        }[x]
    )
    

    
    # Auto refresh settings
    st.sidebar.header("⚙️ Cài đặt tự động")
    
    auto_refresh = st.sidebar.checkbox("Tự động cập nhật", value=True)
    refresh_interval = st.sidebar.slider("Khoảng thời gian (giây)", 10, 300, 30)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Cập nhật ngay", type="primary"):
        st.rerun()
    
    # Force refresh data button
    if st.sidebar.button("🔄 Làm mới dữ liệu", type="secondary"):
        # Clear cache and rerun
        st.cache_data.clear()
        st.rerun()
    
    # Auto refresh logic
    if auto_refresh:
        # Check for database changes
        if st.session_state.watcher.check_for_changes():
            st.sidebar.markdown('<div class="auto-refresh">🔄 Phát hiện thay đổi - Đang cập nhật...</div>', unsafe_allow_html=True)
            st.rerun()
        
        # Time-based refresh
        time_diff = (datetime.now() - st.session_state.last_update).total_seconds()
        if time_diff >= refresh_interval:
            st.session_state.last_update = datetime.now()
            st.sidebar.markdown('<div class="auto-refresh">⏰ Cập nhật theo thời gian</div>', unsafe_allow_html=True)
            st.rerun()
        
        # Show countdown
        remaining = refresh_interval - time_diff
        st.sidebar.progress(1 - (remaining / refresh_interval))
        st.sidebar.text(f"Cập nhật tiếp theo trong: {remaining:.0f}s")
    




    
    # Main content based on view type
    if dashboard_view == 'sales':
        # Sales Dashboard
        st.header("📊 Báo cáo Bán hàng")
        
        # Product Search Section - Only in sales view
        st.subheader("🔍 Tìm kiếm sản phẩm")
        
        # Create two columns layout
        search_col1, search_col2 = st.columns([1, 2])
        
        with search_col1:
            st.write("📂 **Danh mục**")
            # Get categories for filter
            all_categories = fetch_categories()
            selected_category = st.selectbox(
                "Chọn danh mục:",
                options=['Tất cả'] + all_categories,
                key="category_search"
            )
        
        with search_col2:
            st.write("🔍 **Tìm kiếm sản phẩm**")
            search_keyword = st.text_input(
                "Nhập từ khóa:",
                key="main_search",
                placeholder="Ví dụ: bánh, cà phê, trà..."
            )
        
        # Hiển thị thông tin bộ lọc hiện tại
        filter_info = []
        if selected_category != 'Tất cả':
            filter_info.append(f"Danh mục: **{selected_category}**")
        if search_keyword:
            filter_info.append(f"Tìm kiếm: **{search_keyword}**")
        
        if filter_info:
            st.info(f"🔍 Bộ lọc đang áp dụng: {' | '.join(filter_info)} | Thời gian: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}")
        else:
            st.info(f"📊 Hiển thị tất cả dữ liệu | Thời gian: {start_date.strftime('%d/%m/%Y')} - {end_date.strftime('%d/%m/%Y')}")
        st.divider()
        
        # KPI Section - Áp dụng bộ lọc
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        
        with kpi_col1:
            # Áp dụng bộ lọc cho tổng sản phẩm
            filtered_products = fetch_all_products(selected_category, search_keyword)
            total_products = len(filtered_products) if not filtered_products.empty else 0
            st.metric("Tổng sản phẩm", f"{total_products:,}")
        
        with kpi_col2:
            # Áp dụng bộ lọc cho doanh thu
            category_param = selected_category if selected_category != 'Tất cả' else 'all'
            total_revenue = fetch_total_revenue(start_date, end_date, category_param, price_range, search_keyword)
            st.metric("Tổng doanh thu", f"{total_revenue:,.0f}₫")
        
        with kpi_col3:
            # Áp dụng bộ lọc cho số lượng đã bán
            total_sold = fetch_total_sold(start_date, end_date, selected_category, search_keyword)
            st.metric("Đã bán", f"{total_sold:,}")
            
            # Thông báo nếu có sản phẩm nhưng không có doanh số
            if total_products > 0 and total_sold == 0:
                st.warning("⚠️ Có sản phẩm nhưng chưa có doanh số trong khoảng thời gian này")
        
        with kpi_col4:
            # Hiển thị số danh mục được lọc
            if selected_category != 'Tất cả':
                st.metric("Danh mục", "1")
            else:
                all_categories = fetch_categories()
                st.metric("Tổng danh mục", f"{len(all_categories):,}")
        
        # Charts Section
        st.header("📈 Biểu đồ phân tích")
        
        # Revenue by Category Charts
        st.subheader("📊 Doanh thu theo danh mục theo thời gian")
        
        # Time period selector
        col1, col2, col3 = st.columns([2, 2, 1])
        with col1:
            stack_start_date = st.date_input("Từ ngày:", start_date, key="stack_start")
        with col2:
            stack_end_date = st.date_input("Đến ngày:", end_date, key="stack_end")
        with col3:
            stack_view_type = st.selectbox("Xem theo:", ['day', 'week', 'month', 'year'], 
                                         format_func=lambda x: {'day': 'Ngày', 'week': 'Tuần', 'month': 'Tháng', 'year': 'Năm'}[x],
                                         key="stack_view")
        
        # Fetch data
        stack_df = fetch_revenue_by_category_time(stack_view_type, stack_start_date, stack_end_date)
        
        if not stack_df.empty:
            # Process data for Top 5 + Others
            category_totals = stack_df.groupby('category')['revenue'].sum().sort_values(ascending=False)
            top5_categories = category_totals.head(5).index.tolist()
            
            # Create "Khác" category for remaining categories
            stack_df_processed = stack_df.copy()
            stack_df_processed.loc[~stack_df_processed['category'].isin(top5_categories), 'category'] = 'Khác'
            stack_df_processed = stack_df_processed.groupby(['period', 'category'])['revenue'].sum().reset_index()
            
            title_map = {'day': 'ngày', 'week': 'tuần', 'month': 'tháng', 'year': 'năm'}
            
            # 1. Stacked Area Chart
            st.write("**📈 Stacked Area Chart - Tỷ trọng đóng góp (Top 5 + Khác)**")
            fig_area = px.area(
                stack_df_processed,
                x='period',
                y='revenue',
                color='category',
                title=f"Tỷ trọng doanh thu theo {title_map[stack_view_type]} (Top 5 + Khác)",
                labels={'revenue': 'Doanh thu (₫)', 'period': 'Thời gian', 'category': 'Danh mục'}
            )
            fig_area.update_layout(
                height=400,
                font=dict(size=12),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            if stack_view_type == 'day':
                fig_area.update_xaxes(tickangle=45)
            st.plotly_chart(fig_area, use_container_width=True)
            
            # 2. Multi-line Chart
            st.write("**📊 Multi-line Chart - Xu hướng từng danh mục**")
            fig_line = px.line(
                stack_df_processed,
                x='period',
                y='revenue',
                color='category',
                title=f"Xu hướng doanh thu theo {title_map[stack_view_type]} (Top 5 + Khác)",
                labels={'revenue': 'Doanh thu (₫)', 'period': 'Thời gian', 'category': 'Danh mục'},
                markers=True
            )
            fig_line.update_layout(
                height=400,
                font=dict(size=12),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
            )
            if stack_view_type == 'day':
                fig_line.update_xaxes(tickangle=45)
            st.plotly_chart(fig_line, use_container_width=True)
            
            # Summary stats
            period_totals = stack_df_processed.groupby('period')['revenue'].sum().reset_index()
            if not period_totals.empty:
                max_period = period_totals.loc[period_totals['revenue'].idxmax()]
                period_type_map = {'day': 'Ngày', 'week': 'Tuần', 'month': 'Tháng', 'year': 'Năm'}
                st.success(f"🏆 **{period_type_map[stack_view_type]} bán cao nhất**: {max_period['period']} với doanh thu {max_period['revenue']:,.0f}₫")
                
                # Top categories info
                st.info(f"📊 **Top 5 danh mục**: {', '.join(top5_categories[:5])}")
        else:
            st.info("📊 Không có dữ liệu doanh thu trong khoảng thời gian đã chọn")
        
        st.divider()
        
        # Kiểm tra và cảnh báo nếu cần mở rộng thời gian
        if total_products > 0 and total_sold == 0 and total_revenue == 0:
            st.warning("📅 **Gợi ý**: Có thể dữ liệu bán hàng nằm ngoài khoảng thời gian hiện tại. Hãy thử mở rộng 'Từ ngày' trong sidebar.")

        st.subheader("📊 Sản phẩm bán chạy")
        sellers_df = fetch_best_worst_sellers(start_date, end_date, 10, selected_category, search_keyword)
        if not sellers_df.empty:
            # Hiển thị cả sản phẩm chưa bán (total_sold = 0)
            fig_sellers = px.bar(
                sellers_df,
                x='total_sold',
                y='product_name',
                orientation='h',
                title="Top 10 sản phẩm bán chạy",
                color='total_sold',
                color_continuous_scale='viridis',
                text='total_sold'  # Thêm số liệu vào cột
            )
            fig_sellers.update_layout(
                height=600,
                showlegend=False,
                yaxis={'categoryorder': 'total ascending'},
                font=dict(size=14),
                title_font_size=18
            )
            fig_sellers.update_traces(
                texttemplate='%{text}',  # Hiển thị số liệu
                textposition='outside'  # Hiển thị bên ngoài cột
            )
            st.plotly_chart(fig_sellers, use_container_width=True)
            
            # Thông báo về số sản phẩm chưa bán
            zero_sales = len(sellers_df[sellers_df['total_sold'] == 0])
            total_with_sales = len(sellers_df[sellers_df['total_sold'] > 0])
            
            if zero_sales > 0:
                st.info(f"📊 Có {total_with_sales} sản phẩm có doanh số, {zero_sales} sản phẩm chưa bán trong khoảng thời gian này")
            else:
                st.success(f"🎉 Tất cả {len(sellers_df)} sản phẩm đều có doanh số bán hàng!")
        else:
            st.info("Không có dữ liệu để hiển thị")


        
        # Trend chart
        st.subheader("📈 Xu hướng bán hàng")

        trend_df = fetch_sales_trend(view_type, start_date, end_date, selected_category, search_keyword)

        if not trend_df.empty:
            # Xử lý dữ liệu theo view_type
            if view_type == 'day':
                trend_df['period'] = pd.to_datetime(trend_df['period'], format='%Y-%m-%d', errors='coerce')
                x_title = "Ngày"
                tickformat = "%Y-%m-%d"
            elif view_type == 'month':
                trend_df['period'] = pd.to_datetime(trend_df['period'] + '-01', format='%Y-%m-%d', errors='coerce')
                x_title = "Tháng"
                tickformat = "%Y-%m"
            else:  # year
                trend_df['period'] = pd.to_datetime(trend_df['period'] + '-01-01', format='%Y-%m-%d', errors='coerce')
                x_title = "Năm"
                tickformat = "%Y"

            trend_df = trend_df.dropna(subset=['period'])
            trend_df = trend_df.sort_values('period').reset_index(drop=True)
            trend_df['revenue'] = pd.to_numeric(trend_df['revenue'], errors='coerce')
            trend_df['revenue'] = trend_df['revenue'].fillna(0)

            if len(trend_df) > 0:
                # Tạo biểu đồ kết hợp cột và đường
                fig = go.Figure()
                
                # Thêm biểu đồ cột
                fig.add_trace(go.Bar(
                    x=trend_df['period'],
                    y=trend_df['revenue'],
                    name='Doanh thu (Cột)',
                    marker_color='lightblue',
                    opacity=0.7
                ))
                
                # Thêm biểu đồ đường
                fig.add_trace(go.Scatter(
                    x=trend_df['period'],
                    y=trend_df['revenue'],
                    mode='lines+markers',
                    name='Xu hướng (Đường)',
                    line=dict(color='red', width=3),
                    marker=dict(size=8, color='red')
                ))
                
                fig.update_layout(
                    title=f"Xu hướng doanh thu theo {x_title.lower()}",
                    height=600,
                    xaxis_title=x_title,
                    yaxis_title="Doanh thu (₫)",
                    font=dict(size=14),
                    title_font_size=18,
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    )
                )
                fig.update_xaxes(tickformat=tickformat, title_text=x_title)
                fig.update_yaxes(tickformat=',.0f', title_text="Doanh thu (₫)")
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning(f"Không có dữ liệu hợp lệ để hiển thị biểu đồ đường theo {x_title.lower()}.")
        else:
            st.info("Không có dữ liệu bán hàng trong khoảng thời gian được chọn.")
            
        # Price segment chart
        st.subheader("🎯 Phân khúc giá")
        price_df = fetch_price_segments_kmeans(selected_category, search_keyword)
        if not price_df.empty:
            # Kiểm tra nếu có ít nhất một phân khúc có doanh số > 0
            if price_df['total_sold'].sum() > 0:
                # Tạo donut chart thay vì pie chart
                fig_price = go.Figure(data=[go.Pie(
                    labels=price_df['price_segment'],
                    values=price_df['total_sold'],
                    hole=0.4,  # Tạo lỗ ở giữa để thành donut
                    marker_colors=['#FF6B6B', '#4ECDC4', '#45B7D1'],
                    textposition='inside',
                    textinfo='percent+label',
                    textfont_size=14
                )])
                fig_price.update_layout(
                    title="Phân bố bán hàng theo phân khúc giá",
                    height=600,
                    font=dict(size=14),
                    title_font_size=18,
                    legend=dict(
                        font=dict(size=16),
                        x=1.05,
                        y=0.5,
                        xanchor='left',
                        yanchor='middle'
                    )
                )
                st.plotly_chart(fig_price, use_container_width=True)
            else:
                st.warning("⚠️ Danh mục này có sản phẩm nhưng chưa có doanh số bán hàng")
                # Hiển thị thông tin về số lượng sản phẩm theo phân khúc giá
                st.write("**Phân bố sản phẩm theo giá:**")
                for _, row in price_df.iterrows():
                    st.write(f"- {row['price_segment']}: {row['product_count']} sản phẩm (chưa có doanh số trong khoảng thời gian này)")
                st.info("📅 **Gợi ý**: Hãy thử mở rộng khoảng thời gian hoặc kiểm tra phân khúc giá trong sidebar")
        else:
            st.info("Không có dữ liệu để hiển thị")
        
        # Category analysis charts
        st.subheader("📊 Phân tích theo danh mục")
        category_df = fetch_category_analysis(selected_category, search_keyword)
        if not category_df.empty and len(category_df) > 1:
            # Bar chart - Revenue by category (full width, larger)
            fig_bar = px.bar(
                category_df,
                x='category',
                y='total_revenue',
                title="Doanh thu theo danh mục",
                color='total_revenue',
                color_continuous_scale='Reds',
                text='total_revenue'  # Thêm số liệu lên cột
            )
            fig_bar.update_layout(
                height=500,  # Tăng chiều cao
                xaxis_tickangle=-45,
                font=dict(size=14),
                title_font_size=18,
                showlegend=False  # Ẩn color bar
            )
            fig_bar.update_coloraxes(showscale=False)  # Ẩn thanh màu total_revenue
            fig_bar.update_traces(
                texttemplate='%{text:,.0f}₫',  # Format số liệu
                textposition='outside'  # Hiển thị bên ngoài cột
            )
            st.plotly_chart(fig_bar, use_container_width=True)
            
            # Thêm thông tin hỗ trợ cho nhiều danh mục
            if len(category_df) > 10:
                st.info(f"📈 Hiển thị {len(category_df)} danh mục. Sử dụng bộ lọc để thu hẹp kết quả nếu cần.")
            
            # Bar chart ngang - Revenue by category (better for many categories)
            fig_horizontal = px.bar(
                category_df,
                x='total_revenue',
                y='category',
                orientation='h',
                title="Doanh thu theo danh mục (Bar Chart Ngang)",
                color='total_revenue',
                color_continuous_scale='Blues',
                text='total_revenue'
            )
            fig_horizontal.update_layout(
                height=max(400, len(category_df) * 25),  # Động điều chỉnh chiều cao theo số danh mục
                font=dict(size=12),
                title_font_size=18,
                yaxis={'categoryorder': 'total ascending'},  # Sắp xếp theo giá trị
                margin=dict(l=150, r=50, t=60, b=40),  # Margin cho nhãn dài
                showlegend=False  # Ẩn legend
            )
            fig_horizontal.update_coloraxes(showscale=False)  # Ẩn thanh màu total_revenue
            fig_horizontal.update_traces(
                texttemplate='%{text:,.0f}₫',
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>Doanh thu: %{x:,.0f}₫<extra></extra>'
            )
            fig_horizontal.update_xaxes(title_text="Doanh thu (₫)")
            fig_horizontal.update_yaxes(title_text="Danh mục")
            st.plotly_chart(fig_horizontal, use_container_width=True)
        else:
            st.info("Cần ít nhất 2 danh mục để hiển thị biểu đồ so sánh")
            # Hiển thị thông tin nếu chỉ có 1 danh mục
            if not category_df.empty and len(category_df) == 1:
                st.write(f"**Danh mục duy nhất**: {category_df.iloc[0]['category']} - Doanh thu: {category_df.iloc[0]['total_revenue']:,.0f}₫")
        
        # Hiển thị thống kê tổng quan
        if not category_df.empty:
            total_categories = len(category_df)
            total_revenue_all = category_df['total_revenue'].sum()
            avg_revenue = total_revenue_all / total_categories
            st.info(f"📊 **Tổng quan**: {total_categories} danh mục - Tổng doanh thu: {total_revenue_all:,.0f}₫ - Trung bình: {avg_revenue:,.0f}₫/danh mục")

        
        # Additional sections for sales view only
        st.header("📋 Báo cáo chi tiết")
        
        # Bộ lọc ngày chung cho báo cáo chi tiết
        col_date1, col_date2, col_date3 = st.columns([2, 2, 1])
        with col_date1:
            # Mặc định hiển thị từ ngày 5/9/2025
            default_start = datetime(2025, 9, 5).date()
            start_date_detail = st.date_input("Từ ngày:", default_start, key="start_date_detail")
        with col_date2:
            end_date_detail = st.date_input("Đến ngày:", datetime.now(), key="end_date_detail")



        detail_col1, detail_col2 = st.columns(2)
        
        with detail_col1:
            st.subheader("📈 Lịch sử thay đổi giá")
            # Pass the date range directly to the fetch function
            price_history_display_df = fetch_all_price_history(start_date_detail, end_date_detail, selected_category, search_keyword)
            if not price_history_display_df.empty:
                # Ensure 'Ngày' column is datetime for proper display and further processing if needed
                price_history_display_df['Ngày'] = pd.to_datetime(price_history_display_df['Ngày'], errors='coerce')
                
                # The dataframe is already filtered by SQL, so just use it directly
                filtered_price_display = price_history_display_df
                
                if not filtered_price_display.empty:
                    st.dataframe(filtered_price_display, use_container_width=True)
                    st.info(f"📊 Có {len(filtered_price_display)} thay đổi giá trong khoảng thời gian đã chọn")
                else:
                    # The warning message should reflect that no data was found for the *selected* range
                    st.warning(f"""
                    ⚠️ **Không có dữ liệu lịch sử giá trong khoảng ngày đã chọn ({start_date_detail.strftime('%d/%m/%Y')} - {end_date_detail.strftime('%d/%m/%Y')})**
                    """)
            else:
                st.info("Không có dữ liệu lịch sử giá")
        
        with detail_col2:
            st.subheader("📦 Lịch sử thay đổi tồn kho")
            # Pass the date range directly to the fetch function
            stock_changes_display_df = fetch_all_stock_history(start_date_detail, end_date_detail, selected_category, search_keyword)
            if not stock_changes_display_df.empty:
                # Ensure 'Ngày' column is datetime for proper display and further processing if needed
                stock_changes_display_df['Ngày'] = pd.to_datetime(stock_changes_display_df['Ngày'], errors='coerce')
                
                # The dataframe is already filtered by SQL, so just use it directly
                filtered_stock_display = stock_changes_display_df
                
                if not filtered_stock_display.empty:
                    st.dataframe(filtered_stock_display, use_container_width=True)
                    st.info(f"📊 Có {len(filtered_stock_display)} thay đổi tồn kho trong khoảng thời gian đã chọn")
                else:
                    # The warning message should reflect that no data was found for the *selected* range
                    st.warning(f"""
                    ⚠️ **Không có dữ liệu thay đổi tồn kho trong khoảng thời gian đã chọn ({start_date_detail.strftime('%d/%m/%Y')} - {end_date_detail.strftime('%d/%m/%Y')})**
                    """)
            else:
                st.info("Không có dữ liệu thay đổi tồn kho")
        


    elif dashboard_view == 'inventory':
        # Inventory Dashboard - Only inventory report and details
        st.header("📦 Báo cáo Tồn kho")
        
        # Inventory KPIs
        kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
        
        with kpi_col1:
            total_stock = fetch_total_stock()
            st.metric("Tổng tồn kho", f"{total_stock:,}")
        
        # Fetch inventory data once and reuse
        inventory_df = fetch_inventory_status()
        
        with kpi_col2:
            out_of_stock = len(inventory_df[inventory_df['status'] == 'Hết hàng']) if not inventory_df.empty else 0
            st.metric("Hết hàng", f"{out_of_stock:,}", delta=f"-{out_of_stock}" if out_of_stock > 0 else "0")
        
        with kpi_col3:
            low_stock = len(inventory_df[inventory_df['status'].isin(['Sắp hết', 'Tồn kho thấp'])]) if not inventory_df.empty else 0
            st.metric("Tồn kho thấp", f"{low_stock:,}", delta=f"-{low_stock}" if low_stock > 0 else "0")
        
        with kpi_col4:
            good_stock = len(inventory_df[inventory_df['status'] == 'Tồn kho tốt']) if not inventory_df.empty else 0
            st.metric("Tồn kho tốt", f"{good_stock:,}", delta=f"+{good_stock}" if good_stock > 0 else "0")
        
        # Inventory details table
        if not inventory_df.empty:
            st.subheader("📋 Chi tiết tồn kho")
            
            # Filter by status
            status_filter = st.selectbox(
                "Lọc theo trạng thái:",
                options=['Tất cả'] + list(inventory_df['status'].unique()),
                key='status_filter'
            )
            
            if status_filter != 'Tất cả':
                filtered_df = inventory_df[inventory_df['status'] == status_filter]
            else:
                filtered_df = inventory_df
            
            # Display table without color coding
            st.dataframe(
                filtered_df,
                use_container_width=True
            )
        else:
            st.info("Không có dữ liệu tồn kho để hiển thị")



if __name__ == "__main__":
    main()