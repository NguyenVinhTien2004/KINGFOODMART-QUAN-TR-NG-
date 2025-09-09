import rpa as r
import tagui as t
import random
import time
from urllib.parse import urlparse
from test import fetch_and_save_products

def random_sleep(lower_limit, upper_limit):
    if lower_limit > upper_limit:
        raise ValueError("Lower limit cannot be greater than upper limit.")
    sleep_seconds = random.uniform(lower_limit, upper_limit)
    time.sleep(sleep_seconds)
    return sleep_seconds

# r.init(visual_automation=True)
result = r.init(visual_automation=True)                                             
print("Init result:", result)
# Skip IP check
my_ip = "unknown"
# my_new_ip = r.load("ip_proxy.txt")
category_urls = r.load("category_url.txt").splitlines()
random.shuffle(category_urls)
try:   
    for url in category_urls:
        try:
            r.url(url)
            parsed_url = urlparse(url)
            slug = parsed_url.path.strip("/").split("/")[-1]
            print(f"Slug: {slug}")
            r.wait(random_sleep(1.5, 2))
            
            # Thử nhiều selector cho total products
            total_products_str = ""
            selectors = [
                '//*[@id="__next"]/div[1]/main/div/div[4]/div[2]/div[2]/div/div[6]/div[1]/span',
                '//span[contains(text(), "sản phẩm")]',
                '//span[contains(@class, "total")]'
            ]
            
            for selector in selectors:
                try:
                    total_products_str = r.read(selector)
                    if total_products_str and total_products_str.strip():
                        break
                except:
                    continue
            
            # Lấy số từ string
            import re
            numbers = re.findall(r'\d+', total_products_str)
            total_products = int(numbers[0]) if numbers else 102
            
            print(f"Total products: {total_products}")
            r.wait(random_sleep(2, 3))
            fetch_and_save_products(start_page=1, end_page=3, limit_value=total_products, slug_value=slug)
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
            # Tiếp tục với URL tiếp theo
            continue

except Exception as e:
    print(f"Error URL {url}: {str(e)}")
finally:
    r.close()

