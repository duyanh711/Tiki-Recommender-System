from dags.extract.minio_manager import MinIOHandler, connect_minio
from typing import Dict, List, Optional, Tuple, Callable, Any


_WARRANTY_PERIOD_KEY = 'thời_gian_bảo_hành'
_WARRANTY_TYPE_KEY = 'hình_thức_bảo_hành'
_WARRANTY_LOCATION_KEY = 'nơi_bảo_hành'

class TikiTransformer(MinIOHandler):
    def __init__(self, minio_config, tmp_dir="./tmp", root_dir="bronze/tiki"):
        super().__init__(minio_config, tmp_dir, root_dir)

        self._root_dir = root_dir
        self.minio_config = minio_config

        @staticmethod
        def parse_product(json_data: Dict[str, Any]) -> Dict[str, Any]:
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

            # Parse warranty info
            warranty_mapping = {
                'thời_gian_bảo_hành': 'warranty_period',
                'hình_thức_bảo_hành': 'warranty_type',
                'nơi_bảo_hành': 'warranty_location'
            }
            
            for item in json_data.get('warranty_info', []):
                name = item.get('name', '').lower().replace(" ", "_")
                if name in warranty_mapping:
                    result[warranty_mapping[name]] = item.get('value')

            return result