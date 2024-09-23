import requests
from lxml import etree
import csv
import json
import os
import concurrent.futures
import threading
import time

def fetch_and_parse(url):
    # 设置代理
    proxies = {
        'http': 'http://brd-customer-hl_230da8b9-zone-zone1:ev9vt8xccsgg@brd.superproxy.io:22225',
        'https': 'http://brd-customer-hl_230da8b9-zone-zone1:ev9vt8xccsgg@brd.superproxy.io:22225'
    }
    
    use_proxy = False
    while True:
        try:
            # 请求页面并跟随 301 重定向，默认不使用代理
            if use_proxy:
                response = requests.get(url, allow_redirects=True, proxies=proxies)
            else:
                response = requests.get(url, allow_redirects=True)
            
            # 如果状态码是 404，直接返回空
            if response.status_code == 404:
                return None
            
            response.raise_for_status()  # 检查请求是否成功
            break  # 如果请求成功，跳出循环
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("遇到 429 错误，等待 10 秒后使用代理重试...")
                time.sleep(10)
                use_proxy = False
            else:
                raise  # 如果是其他 HTTP 错误，则抛出异常
    
    final_url = response.url  # 获取跳转后的最终 URL
    print(f"最终 URL: {final_url}")  # 打印最终 URL 以供调试
    # print(response.content)
    
    # 解析 HTML
    return etree.HTML(response.content)

def scrape_us_app_page(app_id):
    url = f"https://apps.apple.com/us/app/{app_id}"
    tree = fetch_and_parse(url) 
    if tree is None:
        return {
            'app_id': app_id,
            'app_name': "N/A",
            'subtitle': "N/A",
            'developer_name': "N/A",
            'category': "N/A",
            'date_published': "N/A",
            'last_update': "N/A",
            'rating': "N/A",
            'rating_count': "N/A",
            'developer_website': "N/A",
            'support_website': "N/A",
            'in_app_purchases': "N/A"

        }
    # 抓取信息
    app_name = tree.xpath('//h1/text()')[0].strip()
    subtitle = tree.xpath('//h2[@class="product-header__subtitle app-header__subtitle"]/text()')
    developer_name = tree.xpath('//h2[@class="product-header__identity app-header__identity"]/a/text()')[0].strip()
    category = tree.xpath('//div[@class="information-list__item l-column small-12 medium-6 large-4 small-valign-top"]//dt[contains(text(), "Category")]/following-sibling::dd/a/text()')[0].strip()
    date_published = json.loads(tree.xpath('//script[@type="application/ld+json"]')[0].text)['datePublished']
    last_update_elements = tree.xpath('//time[@data-test-we-datetime]/@datetime')
    last_update = last_update_elements[0].split('T')[0] if last_update_elements else "n/a"
    rating = tree.xpath('//script[@type="application/ld+json"]')[0].text
    rating_data = json.loads(rating)
    rating = rating_data.get('aggregateRating', {}).get('ratingValue', 'N/A')
    rating_count = rating_data.get('aggregateRating', {}).get('reviewCount', 'N/A')
    developer_website = tree.xpath('//ul[@class="inline-list inline-list--app-extensions"]/li[1]/a/@href')[0].strip() if tree.xpath('//ul[@class="inline-list inline-list--app-extensions"]/li[1]/a/@href') else "N/A"
    support_website = tree.xpath('//ul[@class="inline-list inline-list--app-extensions"]/li[2]/a/@href')[0].strip() if tree.xpath('//ul[@class="inline-list inline-list--app-extensions"]/li[2]/a/@href') else "N/A"
    in_app_purchases = "Yes" if tree.xpath('//li[@class="inline-list__item inline-list__item--bulleted app-header__list__item--in-app-purchase"][contains(text(), "Offers In-App Purchases")]') else "No"
    # 有些字段可能为空，所以需要处理
    subtitle = subtitle[0].strip() if subtitle else ""

    return {
        'app_id': app_id,
        'app_name': app_name,
        'subtitle': subtitle,
        'developer_name': developer_name,
        'category': category,
        'date_published': date_published,
        'last_update': last_update,
        'rating': rating,
        'rating_count': rating_count,
        'developer_website': developer_website,
        'support_website': support_website,
        'in_app_purchases': in_app_purchases
    }

def scrape_ee_app_page(app_id):
    url = f"https://apps.apple.com/ee/app/{app_id}"
    tree = fetch_and_parse(url)
    if tree is None:
        return {
            'provider': 'n/a',
            'address': 'n/a',
            'phone_number': 'n/a',
            'email': 'n/a'
        }

    # 抓取信息
    provider = tree.xpath('//dt[contains(text(), "Provider")]/following-sibling::dd/text()')[0].strip()
    provider = provider.split('has identified')[0].strip()
    address_elements = tree.xpath('//dt[contains(text(), "Address")]/following-sibling::dd/text()')
    address = address_elements[0].strip() if address_elements else "n/a"
    phone_elements = tree.xpath('//dt[contains(text(), "Phone")]/following-sibling::dd/text()')
    phone_number = phone_elements[0].strip() if phone_elements else "n/a"
    email_elements = tree.xpath('//dt[contains(text(), "Email")]/following-sibling::dd/text()')
    email = email_elements[0].strip() if email_elements else "n/a"

    return {
        'provider': provider,
        'address': address,
        'phone_number': phone_number,
        'email': email
    }

def save_to_csv(data, filename='gmail_app.csv'):
    keys = data.keys()
    file_exists = os.path.isfile(filename)
    with open(filename, 'a', newline='', encoding='utf-8') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        if not file_exists:
            dict_writer.writeheader()
        dict_writer.writerow(data)

csv_lock = threading.Lock()

def process_app(app_id):
    us_data = scrape_us_app_page(app_id)
    ee_data = scrape_ee_app_page(app_id)
    result = {**us_data, **ee_data}
    
    try:
        with csv_lock:
            save_to_csv(result)
    except Exception as e:
        print(f"保存数据时出错: {e}")
    finally:
        print(f"已完成 {app_id} 的处理")
    print(f"已完成 {app_id} 的抓取并保存")
    return result

def main(app_ids):
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_app_id = {executor.submit(process_app, app_id): app_id for app_id in app_ids}
        for future in concurrent.futures.as_completed(future_to_app_id):
            app_id = future_to_app_id[future]
            try:
                future.result()
            except Exception as exc:
                print(f"{app_id} 抓取出错: {exc}")


if __name__ == '__main__':
    with open('/Users/freeman/Downloads/gmail_id.txt', 'r') as f:
        app_ids = [line.strip() for line in f if line.strip()]
    main(app_ids)
