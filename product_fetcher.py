import os
import requests
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv
import json

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
url = os.getenv("API_URL")

client = MongoClient(MONGO_URI)
db = client["db_kf"]
collection = db["kf_new"] 

headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}

limit_value = 102
slug_value = "bua-an-san-tien-loi"

payload = {
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
        "limit": limit_value,
        "page": None, 
        "slug": slug_value,
        "filters": []
    }
}

def fetch_and_save_products(start_page, end_page, limit_value, slug_value):
    total_products = 0
    for page in range(start_page, end_page + 1):
        payload["variables"]["page"] = page
        payload["variables"]["limit"] = limit_value
        payload["variables"]["slug"] = slug_value
        
        print(f"Fetching data from page {page}...")
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            with open(f"response_page_{page}.json", "w", encoding="utf-8") as f:
              f.write(response.text)
            data = response.json()
            products = data["data"]["listingProductsBySlug"]["data"]
            if not products:
                print(f"Trang {page} không có dữ liệu, dừng lại.")
                break

            today_date = datetime.now().strftime("%Y-%m-%d")
            
            for product in products:
                total_products += 1
                if product:
                    description = product.get("descriptionJson", {}).get("introduction", "")
                else:
                    description = ""

                giftItems = product.get("giftItems") or []
                promotion = "Không có khuyến mãi" 

                if giftItems:  
                    for gift_item in giftItems:
                        promotion_info = gift_item.get("promotionInfo")
                        
                        if promotion_info: 
                            promotion = promotion_info.get("promotionSummary", "Không có khuyến mãi")
                        else:
                            promotion = "Không có khuyến mãi"
                        
                        print(promotion)
                        
                product_variants = product.get("variants", [])
                

                for variant in product_variants:
                    product_id = variant["id"]
                    total_sold = variant.get("orderedCounter", 0)
                    stock_quantity = variant["stockItem"]["quantity"]
                    # print(stock_quantity)
                    original_price = variant["originalPrice"]
                    price = variant["discountPrice"]

                    existing_product = collection.find_one({"id": product_id})
                    sales_history = []
                    stock_history = []
                    price_history = []
                    product_data = {}

                    if existing_product:
                        product_data = existing_product
                        sales_history = existing_product.get("sales_history", [])
                        stock_history = existing_product.get("stock_history", [])
                        price_history = existing_product.get("price_history", [])
                        
                  
                        if sales_history and sales_history[-1]["date"] == today_date:
                            previous_total_sold = sales_history[-1]["total_sold"]
                           
                            new_sold_today = max(0, total_sold - previous_total_sold)
                            sales_history[-1]["sold_in_date"] += new_sold_today
                            sales_history[-1]["total_sold"] = total_sold
                        else:
                            new_sold_today = max(0, total_sold - (sales_history[-1]["total_sold"] if sales_history else 0))
                            sales_history.append({
                                "date": today_date,
                                "total_sold": total_sold,
                                "sold_in_date": new_sold_today,
                            })
                        
                        if stock_history and stock_history[-1]["date"] == today_date:
                            previous_stock = stock_history[-1]["stock_quantity"]
                            print("previous_stock", previous_stock)
                            change = stock_quantity - previous_stock
                            stock_history[-1]["stock_quantity"] = stock_quantity
                            stock_history[-1]["stock_increased"] += max(0, change)
                            stock_history[-1]["stock_decreased"] += abs(min(0, change))
                        else:
                            stock_history.append({
                                "date": today_date,
                                "stock_quantity": stock_quantity,
                                "stock_increased": 0,
                                "stock_decreased": 0,
                            })
                        
        
                        if price_history and price_history[-1]["date"] == today_date:
                            price_history[-1]["price"] = price
                            price_history[-1]["original_price"] = original_price
                        else:
                            price_history.append({
                                "date": today_date,
                                "price": price,
                                "original_price": original_price,
                            })
                    else:
                        sales_history.append({"date": today_date, "total_sold": total_sold, "sold_in_date": 0})
                        stock_history.append({
                            "date": today_date,
                            "stock_quantity": stock_quantity,
                            "stock_increased": 0,
                            "stock_decreased": 0,
                        })
                        price_history.append({
                            "date": today_date,
                            "price": price,
                            "original_price": original_price,
                        })
                    
                    product_data.update({
                        "id": product_id,
                        "name": variant["name"],
                        "stock_quantity": stock_quantity,
                        "total_sold": total_sold,
                        "price": price,
                        "original_price": original_price,
                        "promotion": promotion,
                        "description": description,
                        "date": today_date,
                        "sales_history": sales_history,
                        "stock_history": stock_history,
                        "price_history": price_history,
                        "category": slug_value 
                    })

                    collection.update_one({"id": product_id}, {"$set": product_data}, upsert=True)

                    print(f"Page {page} - Product {product_id} updated: "
                    f"Sold today {sales_history[-1]['sold_in_date']}, "
                    f"Stock Increase: {stock_history[-1]['stock_increased']}, "
                    f"Stock Decrease: {stock_history[-1]['stock_decreased']}, "
                    f"Price: {price}, Original price: {original_price}")

        else:
            print(f"Request for page {page} failed with status code {response.status_code}: {response.text}")
    print(f"\nTotal number of products retrieved: {total_products}")

# fetch_and_save_products(start_page=1, end_page=2, limit_value=102, slug_value="bua-an-san-tien-loi")

