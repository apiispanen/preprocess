import pandas as pd
import numpy as np
from universal_functions import *
ma_data = pd.read_csv("data/ma.csv")
ma_updated = pd.read_csv("data/ma_updated.csv")
ma_name_corrections = pd.read_csv("corrections_from_cann/ma_name_corrections-HL-New.csv").set_index("Door Name in System")

def ma_processing_function(df):
    df = df.copy()
    df = df.assign(use_date=lambda x: pd.to_datetime(x["start_ship_date"]),
                                    state="MA",
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
                                    #Delivery = lambda x :x["Delivery"],
                                    dollar_vol=lambda x: x["line_item_total"].apply(convert_excel_num_to_float),
                                    assigned_rep=None)
    return df

# Only has door - need to get account brought in
# What is 'Sale Price' in the original data source?
ma_data_with_date = ma_data.pipe(ma_processing_function) \
    .pipe(correct_door_names, ref_dataset=ma_name_corrections, state="MA") \
    .query("use_date >= '2022-01-01 00:00:00'") \
    .query("use_date < '2022-04-01 00:00:00'")


ma_updated_with_date = ma_updated.pipe(ma_processing_function) \
    .pipe(correct_door_names, ref_dataset=ma_name_corrections, state="MA") \
    .query("use_date >= '2022-04-01 00:00:00'")

ma_mar_sep_21_raw = pd.read_csv("output_data/ma_mar_sep_21_extract.csv")
ma_mar_sep_21 = ma_mar_sep_21_raw.assign(use_date=lambda x: pd.to_datetime(x["delivery_date"]),
                                            state="MA",
                                            door=lambda x: x["Name"].apply(lambda x: x.strip()),
                                            account=lambda x: x["Name"].apply(lambda x: x.strip()),
                                            ext_doc_id=None,
                                            ext_doc_type=None,
                                            product_name=lambda x: x["variable"].apply(lambda x: x + " - 8oz - 6pk"),
                                            promo_name=lambda x: x["variable"].apply(lambda x: "staff" if "staff" in x else "promo" if "promo" in x else None),
                                            unit_price=lambda x: x["unit_price"],
                                            quantity=lambda x: x["value"],
                                            full_price=lambda x: x["dollar_val"],
                                            discount=lambda x: 0,
                                            dollar_vol=lambda x: x["dollar_val"],
                                            #Delivery = lambda x:x['Delivery'],
                                            assigned_rep=None
                                            ) \
    .pipe(correct_door_names, ref_dataset=ma_name_corrections, state="MA")

ma_oct_dec_21_raw = pd.read_csv("ma_oct_dec_21.csv")
ma_oct_dec_21_raw = ma_oct_dec_21_raw.ffill()
ma_oct_dec_21_raw["amt_shipped"] = ma_oct_dec_21_raw["Amount"].apply(lambda x: convert_excel_num_to_float(x.split("\n")[0].replace("S", "")))
ma_oct_dec_21_raw["units_shipped"] = ma_oct_dec_21_raw["Shipped"].apply(lambda x: int(x.replace("\nea", "")))

ma_oct_dec_21 = ma_oct_dec_21_raw.assign(use_date=lambda x: pd.to_datetime(x["Received"]),
                                        created_date=lambda x: pd.to_datetime(x["Created"]),
                                            state="MA",
                                            door=lambda x: x["Destination Facility"].apply(lambda x: x.strip()),
                                            account=lambda x: x["Destination Facility"].apply(lambda x: x.strip()),
                                            ext_doc_id=lambda x: x["Manifest"],
                                            ext_doc_type="Manifest",
                                            product_name=lambda x: x["Item"],
                                            promo_name=None,
                                            unit_price=lambda x: x["amt_shipped"] / x["units_shipped"],
                                            quantity=lambda x: x["units_shipped"],
                                            full_price=lambda x: x["amt_shipped"],
                                            discount=lambda x: 0,
                                            dollar_vol=lambda x: x["amt_shipped"],
                                            #Delivery = lambda x:x['Delivery'],
                                            assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=ma_name_corrections, state="MA")



ri_data_raw = pd.read_csv("output_data/ri_data.csv").drop(columns=["Unnamed: 0"])
ri_data_with_date = ri_data_raw.assign(use_date=lambda x: pd.to_datetime(x["Delivery"]),
                                        state="RI",
                                        door=lambda x: x["Name"],
                                        account=lambda x: x["Name"],
                                        ext_doc_id=None,
                                        ext_doc_type="Historic",
                                        product_name=lambda x: x["variable"].apply(lambda x: x + " - 8oz - 6pk"),
                                        promo_name=lambda x: x["variable"].apply(lambda x: "staff" if "staff" in x else "promo" if "promo" in x else None),
                                        unit_price=lambda x: x["unit_price"],
                                        quantity=lambda x: x["value"],
                                        full_price=lambda x: x["dollar_val"],
                                        discount=lambda x: 0,
                                        dollar_vol=lambda x: x["dollar_val"],
                                        assigned_rep=None
                                        )

