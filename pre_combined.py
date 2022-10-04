from pre_ca import *
from pre_il import *
from pre_nv import *
from pre_ma import *
from universal_functions import *
common_cols = ["use_date", "state", "door", "account", "ext_doc_id", "ext_doc_type", "product_name", "promo_name", "unit_price", "quantity", "full_price", "discount", "dollar_vol", "assigned_rep"]

unioned_df = pd.concat([
    ca_to_use[common_cols],
    il_data_with_date[common_cols],
    il_updated_with_date[common_cols],
    ma_data_with_date[common_cols],
    ma_updated_with_date[common_cols],
    ma_mar_sep_21[common_cols],
    ma_oct_dec_21[common_cols],
    nv_data_with_date[common_cols],
    nv_updated_with_date[common_cols],
    nv_apr_jun_21[common_cols],
    nv_jul_dec_20[common_cols],
    nv_jul_dec_21[common_cols],
    il_apr_jun_21[common_cols],
    il_jul_dec_21[common_cols],
    ri_data_with_date[common_cols]
   ]).assign(door=lambda x: x["door"].apply(str))

combined = unioned_df
combined['account'] = combined['account'].astype(str)

combined["sales_rep"] = combined.apply(lambda row: assign_rep(row["assigned_rep"], row["state"], row["account"]), axis=1)


combined
combined_with_manual_sku_map = combined.assign(packout=lambda x: x["product_name"].apply(parse_pack_size),
                                                format=lambda x: x["product_name"].apply(parse_container_size),
                                                flavor=lambda x: x["product_name"].apply(parse_flavor),
                                                lite=lambda x: x["product_name"].apply(is_lite),
                                                sku=lambda x: x["product_name"].apply(determine_sku),
                                                is_sample=lambda x: x["product_name"].apply(is_sample),
                                                is_promotion=lambda x: x["product_name"].apply(is_promotion),
                                                fam_mapping=lambda x: x["flavor"].apply(apply_flavor_to_fam_mapping)
                                                #Delivery  = lambda x:x["Delivery"]
                                                )

print("Writing Out File")
combined_with_manual_sku_map.to_csv("output_data/initial_test3.csv", index=False) 
print("File Written")

