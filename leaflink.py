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

# leaf_req = requests.get(url, headers=headers)

# leaf_json = leaf_req.json()['results']




# with open('leaf_json.json', 'w') as file:
#     file.write(str(leaf_json))

# results = leaf_json['results']
# print(leaf_json)


url = 'https://kiva.encompass8.com/API?APICommand=ReportView&ReportName=All%20Sales&ReportID=13708091&BaseQuery=Sales&Action=Data&ReportIsEdit=True&Format=WebQuery&EncompassID=Kiva&QuickKey=cf5e4e9a278d3c77dbd6492bb0e312db&Parameters=F:ColumnValues~V:Company%5EDate%5EMonths~O:E|F:FieldValues~V:%24Vol~O:E|F:Period~V:ThisYear~O:E|F:YearInt~V:1~O:E|F:CloseDay~V:4~O:E&'

'Report0'
# query = requests.get(url)
# from bs4 import BeautifulSoup as bs
# soup = bs(query.text)
# print(soup.table.prettify())


import pandas as pd
encompass = pd.read_html(url)
encompass[0].to_csv('encompass.csv')
# print(encompass)

# print(query.text)