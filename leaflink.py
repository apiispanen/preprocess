import requests
from pprint import pprint as pp
url = 'https://www.leaflink.com/api/v2/orders-received/'
az_api = '214db35da91443ce2aeb2c1e67704678c0a1623f42d26ccb30561e824040effc'
apothca_token = 'c3def3b1533445a40be909f73d0c24e888a1b945' 
nv_token = 'c3def3b1533445a40be909f73d0c24e888a1b945'

"""
Needed fields:

    use_date	
    state	
    door
        account	
    ext_doc_id	
    ext_doc_type	
    product_name
    promo_name	
    unit_price
    quantity
    full_price
    discount
    dollar_vol
    assigned_rep
    sales_rep
    packout
    format
    flavor
    lite
    sku
    is_sample
    is_promotion
    fam_mapping

"""

use_date = 'created_on'
state = 'MA'
door = 'customer'
account	= 'buyer'
ext_doc_type = 'Order'
product_name = 'brand'
promo_name = 'discount'




headers = { 'Authorization':'Token '+apothca_token}
# params  = {
#     'fields_include': 'buyer'
# }

leaf_req = requests.get(url, headers=headers)

leaf_json = leaf_req.json()['results']

# with open('leaf_json.json', 'w') as file:
#     file.write(str(leaf_json))

# results = leaf_json['results']
# print(leaf_json)


# print([line_item for line_item in results['line_items']])

# print([order['results'] for order in leaf_req.text])