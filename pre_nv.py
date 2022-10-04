import pandas as pd
import numpy as np
from universal_functions import *
nv_data = pd.read_csv("data/nv.csv")



nv_updated = pd.read_csv("data/nv_updated.csv")
nv_name_corrections = pd.read_csv("corrections_from_cann/nv_name_corrections-HL-New.csv").set_index("Door Name in System")


# NV Historical data
nv_jul_dec_20_raw = pd.read_csv("output_data/nv_jul_dec_20_rewrite.csv")

def create_product_name(x):
    size = "12oz" if "highboy" in x else "8oz"
    pack_size = "4pk" if "4pk" in x else "1pk" if "singles" in x else "6pk"
    return x + " - " + size + " - " + pack_size

nv_jul_dec_20 = nv_jul_dec_20_raw.assign(use_date=lambda x: pd.to_datetime(x["Delivery\nDate"]),
                                            state="NV",
                                            door=lambda x: x["Name"],
                                            account=lambda x: x["Name"],
                                            ext_doc_id=None,
                                            ext_doc_type="Historic",
                                            product_name=lambda x: x["variable"].apply(lambda x: create_product_name(x)),
                                            promo_name=lambda x: x["variable"].apply(lambda x: "staff" if "staff" in x else "promo" if "promo" in x else None),
                                            unit_price=lambda x: x["unit_price"].apply(convert_excel_num_to_float),
                                            quantity=lambda x: x["value"].apply(convert_excel_num_to_float),
                                            full_price=lambda x: x["dollar_val"].apply(convert_excel_num_to_float),
                                            discount=lambda x: 0,
                                            dollar_vol=lambda x: x["dollar_val"].apply(convert_excel_num_to_float),
                                            assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=nv_name_corrections, state="NV")

nv_apr_jun_21_raw = pd.read_csv("nv_apr_jun_21.csv")

nv_apr_jun_21 = nv_apr_jun_21_raw.assign(use_date=lambda x: pd.to_datetime(x["Day"]),
                                            state="NV",
                                            door=lambda x: x["Door"],
                                            account=lambda x: x["Account"],
                                            ext_doc_id=lambda x: x["Invoice #"],
                                            ext_doc_type="Invoice",
                                            product_name=lambda x: x["SKU"],
                                            promo_name=None,
                                            unit_price=lambda x: x["Order"].apply(convert_excel_num_to_float),
                                            quantity=lambda x: x["# Units"].apply(convert_excel_num_to_float),
                                            full_price=lambda x: x["Net Sales"].apply(convert_excel_num_to_float),
                                            discount=lambda x: 0,
                                            dollar_vol=lambda x: x["Net Sales.1"].apply(convert_excel_num_to_float),
                                            assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=nv_name_corrections, state="NV")

nv_jul_dec_21_raw = pd.read_csv("nv_jul_dec_21.csv")
nv_jul_dec_21_filtered = nv_jul_dec_21_raw[~nv_jul_dec_21_raw["Product"].isnull() & ~nv_jul_dec_21_raw["Date"].isnull()]

nv_jul_dec_21 = nv_jul_dec_21_filtered.assign(use_date=lambda x: pd.to_datetime(x["Date"]),
                                                state="NV",
                                                door=lambda x: x["Customer"].apply(lambda x: "-".join(x.split("-")[1:]).strip()),
                                                account=lambda x: x["Customer"].apply(lambda x: "-".join(x.split("-")[1:]).strip()),
                                                ext_doc_id=lambda x: x["Num"],
                                                ext_doc_type="Invoice",
                                                product_name=lambda x: x["Product"],
                                                promo_name=None,
                                                unit_price=lambda x: x["Sales Price"].apply(convert_excel_num_to_float),
                                                quantity=lambda x: x["Qty"].apply(convert_excel_num_to_float),
                                                full_price=lambda x: x["Amount"].apply(convert_excel_num_to_float),
                                                discount=lambda x: 0,
                                                dollar_vol=lambda x: x["Amount"].apply(convert_excel_num_to_float),
                                                assigned_rep=None) \
    .query("use_date < '20211201'") \
    .pipe(correct_door_names, ref_dataset=nv_name_corrections, state="NV")




# Same datasource as ma

def proc_nv(df):
    df = df.copy()
    df = df.assign(use_date=lambda x: pd.to_datetime(x["start_ship_date"]),
                                    state="NV",
                                    door=lambda x: x["customer"],
                                    account=None,
                                    ext_doc_id=lambda x: x["orderID"],
                                    ext_doc_type="Order",
                                    product_name=lambda x: x["product"],
                                    promo_name=None,
                                    units_per_case=lambda x: x["Qty (Units)"] / x["Qty (Cases)"],
                                    orig_unit_price=lambda x: x["unit_price"].apply(convert_excel_num_to_float),
                                    unit_price=lambda x: x["orig_unit_price"] / x["units_per_case"],
                                    quantity=lambda x: x["Qty (Units)"],
                                    full_price=lambda x: x["unit_price"] * x["quantity"],
                                    discount=lambda x: 0,
                                    dollar_vol=lambda x: x["line_item_total"].apply(convert_excel_num_to_float),
                                    assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=nv_name_corrections, state="NV")
    return df

nv_data_with_date = nv_data.pipe(proc_nv) \
    .pipe(filter_pre_april_2022)

nv_updated_with_date = nv_updated.pipe(proc_nv) \
    .pipe(filter_post_april_2022)

# print(nv_updated_with_date)