import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

LOG_LEVEL = "INFO"

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT", "localhost:9000"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "your-access-key"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "your-secret-key"),
    "bucket": os.getenv("DATALAKE_BUCKET", "your-bucket-name"),
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi-VN,vi;q=0.8,en-US;q=0.5,en;q=0.3',
    'x-guest-token': 'yj5i8HfLhplN6ckw471WVBG2QAzbTr3a',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

PARAMS_PAGE = {
    'limit': '40',
    'aggregations': '2',
    'trackity_id': 'dd393ec3-561d-b612-c009-277495b0b207',
    'page': '1',
    'category': '1883',
    'urlKey':  'nha-cua-doi-song',
}

PARAMS = {
    "params_product": {
        "platform": "web",
        "version": 3
    },
    "params_reviews": {
        'sort': 'score|desc,id|desc,stars|all',
        'page': 1,
        'limit': 5,
        'include': 'comments,contribute_info,attribute_vote_summary'
    },
    "params_seller": {
        'trackity_id': 'dd393ec3-561d-b612-c009-277495b0b207',
        "platform": "desktop",
        "version": 3
    }
}



BASE_URLS = {
    "base_url": "https://tiki.vn",
    "base_page_url": "https://tiki.vn/api/personalish/v1/blocks/listings",
    "base_product_url": "https://tiki.vn/api/v2/products/{}",
    "base_reviews_url": "https://tiki.vn/api/v2/reviews",
    "base_seller_url": "https://api.tiki.vn/product-detail/v2/widgets/seller"
}