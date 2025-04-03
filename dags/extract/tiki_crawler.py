import os
import requests
import pandas as pd
import time
import random
import tqdm
from bs4 import BeautifulSoup
from minio.error import S3Error
from extract.minio_manager import MinIOHandler, connect_minio
from dags.extract.utils import download_html
from config.settings import HEADERS, BASE_URLS, PARAMS_PAGE, PARAMS
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class TikiCrawler(MinIOHandler):
    def __init__(self, minio_config, root_dir="bronze/tiki/", tmp_dir="./tmp"):
        super().__init__(minio_config, tmp_dir, root_dir)
        self.base_urls = BASE_URLS
        self.headers = HEADERS
        
        self.base_url = BASE_URLS['base_url']
        self.base_page_url = BASE_URLS['base_page_url']
        self.base_product_url = BASE_URLS["base_product_url"]
        self.base_reviews_url = BASE_URLS["base_reviews_url"]
        self.base_seller_url = BASE_URLS["base_seller_url"]

        self.params_page = PARAMS_PAGE
        self.params_product = PARAMS["params_product"]
        self.params_reviews = PARAMS["params_reviews"]
        self.params_seller = PARAMS["params_seller"]

        self.categories_path = "categories.csv"
        self.tracking_ids_path = "tracking_ids.csv"

        self.categories_df = self.fetch_categories(self.categories_path)
        self.tracking_ids_df = self.init_track_ids(self.tracking_ids_path)

    def init_track_ids(self, path):
        minio_path = os.path.join(self.root_dir, path)
        objects = list(self.client.list_objects(self.minio_config["bucket"], prefix=minio_path, recursive=True))
        if not objects:
            tracking_df = pd.DataFrame(columns=['pid', 'spid', 'seller_id', 'category_id', 'slug'])
            self.put_file_to_minio(tracking_df, path, file_type="csv")
        else:
            tracking_df = self.get_file_from_minio(path, file_type="csv")
        return tracking_df

    def tracking_ids(self, id):
        new_id_df = pd.DataFrame([id], columns=self.tracking_ids_df.columns)
        self.tracking_ids_df = pd.concat([self.tracking_ids_df, new_id_df], ignore_index=True)
        self.put_file_to_minio(self.tracking_ids_df, self.tracking_ids_path, file_type="csv")

    def fetch_categories(self, path):
        minio_path = os.path.join(self.root_dir, self.categories_path)
        try:
            self.client.stat_object(self.minio_config["bucket"], minio_path)
            df = self.get_file_from_minio(self.categories_path, file_type="csv")
        except S3Error:
            source = download_html(self.base_url)
            soup = BeautifulSoup(source, 'html.parser')
            cat = soup.find('div', {'class': 'styles__StyledListItem-sc-w7gnxl-0 cjqkgR'})
            sub_cats = cat.find_all('a', {'title': True})
            result = [{'title': sub_cat['title'], 'href': sub_cat['href']} for sub_cat in sub_cats]
            df = pd.DataFrame(result)
            df[['slug', 'category_id']] = df['href'].str.extract(r'/([^/]+)/c(\d+)')
            df.drop(columns=['href'], inplace=True)
            self.put_file_to_minio(df, path, file_type="csv")
        return df

    def fetch_ids(self, urlKey, category, page=10):
        self.params_page['urlKey'] = urlKey
        self.params_page['category'] = category
        name = self.categories_df.loc[self.categories_df['category_id'] == str(category), 'title'].values[0]

        ids = []
        logger.info(f"Fetching products id from category: {name}")
        for i in tqdm(range(1, page + 1), desc="Pages Scraped", unit="page"):
            self.params_page['page'] = i
            response = requests.get(
                self.base_page_url, headers=self.headers, params=self.params_page)

            if response.status_code == 200:
                data = response.json().get('data', [])
                for record in data:
                    ids.append({
                        'pid': record.get('id'),
                        'spid': record.get('seller_product_id'),
                        'seller_id': record.get('seller_id'),
                        'category_id': category,
                        'slug': urlKey
                    })
                time.sleep(random.randint(3, 10))
        return ids

    def fetch_product(self, id):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        url = self.base_product_url.format(pid)
        self.params_product['spid'] = spid
        response = requests.get(url, headers=self.headers, params=self.params_product)
        if response.status_code == 200:
            path = f"{slug}/{pid}_{seller_id}/product_{pid}_{seller_id}.json"
            self.put_file_to_minio(response.json(), path, file_type="json")
            return True
        return False

    def fetch_reviews(self, id, page=10):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        self.params_reviews['product_id'] = pid
        self.params_reviews['spid'] = spid
        self.params_reviews['seller_id'] = seller_id

        for i in range(1, page + 1):
            self.params_reviews['page'] = i
            response = requests.get(self.base_reviews_url, headers=self.headers, params=self.params_reviews)
            if response.status_code == 200:
                try:
                    file = response.json()
                    path = f"{slug}/{pid}_{seller_id}/reviews/reviews_{pid}_{seller_id}_{i}.json"
                    self.put_file_to_minio(file, path, file_type="json")
                except Exception as e:
                    logger.error(f"Error fetching reviews: {e}")
                    pass
            else:
                return False
        return True

    def fetch_seller(self, id):
        pid = id['pid']
        spid = id['spid']
        seller_id = id['seller_id']
        slug = id['slug']

        self.params_seller['mpid'] = pid
        self.params_seller['spid'] = spid
        self.params_seller['seller_id'] = seller_id

        response = requests.get(self.base_seller_url, headers=self.headers, params=self.params_seller)
        if response.status_code == 200:
            path = f"{slug}/{pid}_{seller_id}/seller_{pid}_{seller_id}.json"
            self.put_file_to_minio(response.json(), path, file_type="json")
            return True
        return False

    def scrape_all_category(self, urlKey, category, page=10):
        ids = self.fetch_ids(urlKey, category, page)
        products = []
        for id in tqdm(ids, desc="Processing", unit="product"):
            is_existing = (
                (self.tracking_ids_df['pid'] == id['pid']) &
                (self.tracking_ids_df['seller_id'] == id['seller_id'])
            ).any()

            if not is_existing:
                f1 = self.fetch_product(id)
                f2 = self.fetch_reviews(id)
                f3 = self.fetch_seller(id)
                if f1 and f2 and f3:
                    self.tracking_ids(id)
                    products.append(id)
        
        logger.info(f"{len(products)} products added")
        return products

    def scrape_all(self, page=10):
        categories_level_0 = self.categories_df  # Không có cột 'level', lấy tất cả danh mục
        products = []
        for _, category in categories_level_0.iterrows():
            urlKey = category['slug']
            cat_id = category['category_id']
            products.extend(self.scrape_all_category(urlKey, cat_id, page))
        return products

    def num_products(self):
        objects = self.client.list_objects(self.minio_config["bucket"], prefix=self.root_dir, recursive=True)
        
        level1_folders = set()
        level2_subfolders = {}

        for obj in objects:
            parts = obj.object_name[len(self.root_dir):].split('/')
            if len(parts) > 1:
                level1_folder = parts[0]
                level1_folders.add(level1_folder)
                
                if len(parts) > 2 and parts[1]:
                    level2_folder = f"{level1_folder}/{parts[1]}"
                    if level1_folder not in level2_subfolders:
                        level2_subfolders[level1_folder] = set()
                    level2_subfolders[level1_folder].add(level2_folder)

        total_subfolders = 0
        products_num = []
        for level1_folder, subfolders in level2_subfolders.items():
            products_num.append({level1_folder: len(subfolders)})
            total_subfolders += len(subfolders)

        return products_num, total_subfolders