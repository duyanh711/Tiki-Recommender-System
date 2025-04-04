from dags.extract.minio_manager import MinIOHandler, connect_minio
from typing import Dict, List, Optional, Tuple, Callable, Any
from src.utils.logger import setup_logger


_WARRANTY_PERIOD_KEY = 'thời_gian_bảo_hành'
_WARRANTY_TYPE_KEY = 'hình_thức_bảo_hành'
_WARRANTY_LOCATION_KEY = 'nơi_bảo_hành'

logger = setup_logger(__name__)

class TikiTransformer(MinIOHandler):
    def __init__(self, minio_config, tmp_dir="./tmp", root_dir="bronze/tiki"):
        super().__init__(minio_config, tmp_dir, root_dir)

        self._root_dir = root_dir
        self.minio_config = minio_config

        @staticmethod
        def parse_product(json_data: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            try:
                result = {
                'product_id': json_data.get('id'),
                'product_sku': json_data.get('sku'),
                'product_name': json_data.get('name'),
                'product_url': json_data.get('short_url'),
                'images_url': [image.get('base_url') for image in json_data.get('images', [])],
                'description': json_data.get('description'),
                'original_price': json_data.get('list_price'),
                'discount': json_data.get('discount'),
                'price': json_data.get('price'),
                'discount_rate': json_data.get('discount_rate'),
                'inventory_status': json_data.get('inventory_status'),
                'inventory_type': json_data.get('inventory_type'),
                'quantity_sold': json_data.get('all_time_quantity_sold'),
                'day_ago_created': json_data.get('day_ago_created'),
                'rating_average': json_data.get('rating_average'),
                'review_count': json_data.get('review_count')
                }
            
                specifications = json_data.get('specifications', [])
                key_value_pairs = []
                for spec in specifications:
                    for attr in spec.get('attributes', []):
                        key_value_pairs.append(f"{attr.get('name', '')}: {attr.get('value', '')}")
                result['specifications'] = '\n'.join(key_value_pairs)

                # Parse breadcrumbs
                breadcrumbs = json_data.get('breadcrumbs', [])
                if breadcrumbs:
                    result['breadcrumbs'] = " / ".join(b.get('name') for b in breadcrumbs[:-1])
                    result['category_id'] = breadcrumbs[0].get('category_id')

                # Parse tracking info
                tracking_info = json_data.get('tracking_info', {}).get('amplitude', {})
                result.update({
                    'is_authentic': tracking_info.get('is_authentic'),
                    'is_freeship_xtra': tracking_info.get('is_freeship_xtra'),
                    'is_top_deal': tracking_info.get('is_hero'),
                    'is_top_brand': tracking_info.get('is_top_brand'),
                    'return_reason': tracking_info.get('return_reason')
                })

                # Parse brand info
                brand = json_data.get('brand', {})
                result.update({
                    'brand_id': brand.get('id'),
                    'brand_name': brand.get('name')
                })

                # Parse authors (for books)
                authors = json_data.get('authors', [])
                result['authors'] = [(a['id'], a['name']) for a in authors]

                # Parse seller info
                seller = json_data.get('current_seller', {})
                result.update({
                    'store_id': seller.get('store_id'),
                    'seller_id': seller.get('id'),
                    'seller_sku': seller.get('sku'),
                    'seller_name': seller.get('name'),
                    'seller_url': seller.get('link'),
                    'seller_logo': seller.get('logo')
                })

                
                for item in json_data.get('warranty_info', []):
                    if item:
                        name = str(item.get('name', '')).lower().replace(" ", "_")
                        value = item.get('value')
                        if name == _WARRANTY_PERIOD_KEY:
                            result['warranty_period'] = value
                        elif name == _WARRANTY_TYPE_KEY:
                            result['warranty_type'] = value
                        elif name == _WARRANTY_LOCATION_KEY:
                            result['warranty_location'] = value
            except (AttributeError, KeyError, TypeError, IndexError) as e:
                product_id = json_data.get('id', 'N/A')
                logger.error(f"Error parsing product {product_id}: {e}", exc_info=True)
                return result

            return result
        
        def parse_seller(json_data: Dict[str, Any]) -> Dict[str, Any]:
            result = {}
            seller = json_data.get('data', {}).get('seller', {})

            if not seller:
                logger.warning("Seller data not found in expected structure.")
                return {}
            try:
                result = {
                    'seller_id': seller.get('id'),
                    'seller_name': seller.get('name'),
                    'icon_url': seller.get('icon'),
                    'store_url': seller.get('url'),
                    'avg_rating_point': seller.get('avg_rating_point'),
                    'review_count': seller.get('review_count'),
                    'total_follower': seller.get('total_follower'),
                    'days_since_joined': seller.get('days_since_joined'),
                    'is_official': seller.get('is_official'),
                    'store_level': seller.get('store_level')
                }
            except (AttributeError, KeyError, TypeError) as e:
                seller_id = seller.get('id', 'N/A')
                logger.error(f"Error parsing seller {seller_id}: {e}", exc_info=True)
                return result
            return result
        
        @staticmethod
        def parse_review(json_data: Dict[str, Any]) -> Dict[str, Any]:
            reviews = []
            comments = json_data.get('data', [])
            
            for idx, comment in comments:
                try:
                    review = {
                        'review_id': comment.get('id'),
                        'product_id': comment.get('product_id'),
                        'seller_id': comment.get('seller', {}).get("id"),
                        'title': comment.get('title'),
                        'content': comment.get('content'),
                        'status': comment.get('status'),
                        'thank_count': comment.get('thank_count'),
                        'rating': comment.get('rating'),
                        'created_at': comment.get('created_at'),
                        'customer_id': comment.get('customer_id')
                    }

                    created_by = comment.get('created_by', {})
                    if created_by:
                        review.update({
                            'customer_name': created_by.get('full_name'),
                            'purchased_at': created_by.get('purchased_at'),
                            'avatar_url': created_by.get('avatar_url'),
                            'joined_time': created_by.get('contribute_info', {}).get('summary', {}).get('joined_time'),
                            'total_review': created_by.get('contribute_info', {}).get('summary', {}).get('total_review', 0),
                            'total_thank': created_by.get('contribute_info', {}).get('summary', {}).get('total_thank', 0)
                        })
                    else:
                        review.update({
                            'customer_name': None,
                            'purchased_at': None,
                            'avatar_url': None,
                            'joined_time': None,
                            'total_review': 0,
                            'total_thank': 0
                        })

                    reviews.append(review)
                except Exception as e:
                    review_id = comment.get('id', f'index_{idx}')
                    logger.error(f"Error parsing review {review_id}: {e}", exc_info=True)
                    continue
                
            return reviews