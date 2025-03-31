import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from minio.error import S3Error
from extract.minio_manager import MinIOHandler, connect_minio
from etl.extract.utils import download_html
from config.settings import HEADERS, BASE_URLS

class TikiCrawler(MinIOHandler):
    def __init__(self, minio_config, root_dir="bronze/tiki/", tmp_dir="./tmp"):
        super().__init__(minio_config, tmp_dir, root_dir)
        self.base_urls = BASE_URLS
        self.headers = HEADERS
        self.categories_path = "categories.csv"

    def fetch_categories(self):
        with connect_minio(self.minio_config) as client:
            minio_path = os.path.join(self.root_dir, self.categories_path)
            try:
                client.stat_object(self.minio_config["bucket"], minio_path)
                return self.get_file_from_minio(self.categories_path, file_type="csv")
            except S3Error:
                return self._scrape_categories()

    def _scrape_categories(self):
        source = download_html(self.base_urls["base_url"])
        soup = BeautifulSoup(source, 'html.parser')
        categories = soup.select("div.styles__StyledListItem-sc-w7gnxl-0 a[title]")
        data = [{'title': cat['title'], 'href': cat['href']} for cat in categories]
        df = pd.DataFrame(data)
        df[['slug', 'category_id']] = df['href'].str.extract(r'/([^/]+)/c(\d+)')
        df.drop(columns=['href'], inplace=True)
        self.put_file_to_minio(df, self.categories_path, file_type="csv")
        return df

    def fetch_product(self, pid, slug):
        url = self.base_urls["base_product_url"].format(pid)
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            self._save_json(response.json(), f"{slug}/{pid}/product_{pid}.json")
            return True
        return False

    def scrape_all(self):
        categories = self.fetch_categories()
        for _, category in categories.iterrows():
            slug, cat_id = category['slug'], category['category_id']
            self.fetch_product(cat_id, slug)

    def _save_json(self, data, path):
        self.put_file_to_minio(data, path, file_type="json")
