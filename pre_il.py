import pandas as pd
import numpy as np
from universal_functions import *
il_name_corrections = pd.read_csv("corrections_from_cann/il_name_corrections-HL-New.csv").set_index("Door Name in System")
il_data = pd.read_csv("data/il.csv")  # Leaftrade data
il_updated = pd.read_csv("data/il_updated.csv")


def correct_door_names_func(door, account, ref_dataset, state):
    # print(door)
    usedoor = (account + " - " + door) if state in ["IL"] else door
    if usedoor in ref_dataset.index:
        # print("Triggered")
        new_door = ref_dataset.loc[usedoor]["New Door Name for Database"]
        new_account = ref_dataset.loc[usedoor]["Account"]
        return new_door, new_account, True
    else:
        return door, account, False

def correct_door_names(df, ref_dataset, state):
    df = df.copy()
    df[["door", "account", "adj_triggered"]] = df.apply(lambda row: correct_door_names_func(row["door"], row["account"], ref_dataset, state), axis=1, result_type='expand')
    return df

# Read in historic IL data
def split_acct_and_door(x):
    return [x.strip() for x in x.split("-")]

def filter_pre_april_2022(df):
    df = df.copy()
    return df.query("use_date < Timestamp('2022-04-01 00:00:00')")

def filter_post_april_2022(df):
    df = df.copy()
    return df.query("use_date >= Timestamp('2022-04-01 00:00:00')")

il_apr_jun_21_raw_data = pd.read_csv("output_data/il_apr_jun_21_extract.csv")

acct_and_door = il_apr_jun_21_raw_data["Name"].apply(split_acct_and_door)
il_apr_jun_21_raw_data["account"] = acct_and_door.apply(lambda x: x[0] if len(x) > 1 else "")
il_apr_jun_21_raw_data["door"] = acct_and_door.apply(lambda x: x[1] if len(x) > 1 else x[0])



il_apr_jun_21 = il_apr_jun_21_raw_data.assign(use_date=lambda x: pd.to_datetime(x["delivery_date"]),
                                                state="IL",
                                                door=lambda x: x["door"],
                                                account=lambda x: x["account"],
                                                ext_doc_id=lambda x: x["Invoice"],
                                                ext_doc_type="Invoice",
                                                product_name=lambda x: x["variable"].apply(lambda x: x + " - 8oz - 6pk"),
                                                promo_name=lambda x: x["variable"].apply(lambda x: "staff" if "staff" in x else "promo" if "promo" in x else None),
                                                unit_price=lambda x: x["unit_price"],
                                                quantity=lambda x: x["value"],
                                                full_price=lambda x: x["dollar_val"],
                                                discount=lambda x: 0,
                                                dollar_vol=lambda x: x["dollar_val"],
                                               # Delivery=lambda x:x["Delivery"],
                                                assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=il_name_corrections, state="IL")

il_jul_dec_21_raw_data = pd.read_csv("il_jul_dec_21.csv")
il_jul_dec_21 = il_jul_dec_21_raw_data.assign(use_date=lambda x: pd.to_datetime(x["Day"]),
                                                state="IL",
                                                door=lambda x: x["Customer (Door)"],
                                                account=lambda x: x["Account"],
                                                ext_doc_id=lambda x: x["Invoice #"],
                                                ext_doc_type=lambda x: "Invoice",
                                                product_name=lambda x: x["SKU"],
                                                promo_name=None,
                                                unit_price=lambda x: x["Sale Price"].apply(convert_excel_num_to_float),
                                                quantity=lambda x: x["# Units"].apply(convert_excel_num_to_float),
                                                full_price=lambda x: x["unit_price"] * x["quantity"],
                                                discount=lambda x: 0,
                                                dollar_vol=lambda x: x["full_price"],
                                               # Delivery=lambda x:x["Delivery"],
                                                assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=il_name_corrections, state="IL") \
    .query("use_date < Timestamp('2022-01-01 00:00:00')") # Should be the min of the prior dataset


# Do you want to use order create or delivery date?
# Quantity vs. Stock Quantity.... is Stock their existing inventory?
# Missing discount info?
def proc_il_data(df):
    df = df.copy()
    df = df.assign(use_date=lambda x: pd.to_datetime(x["Delivery Date "]),
                                    state="IL",
                                    door=lambda x: x["Dispensary Location"],
                                    account=lambda x: x["Dispensary Name"],
                                    ext_doc_id=lambda x: x["Order ID"],
                                    ext_doc_type="Order",
                                    product_name=lambda x: x["Product Name"],
                                    promo_name=None,
                                    unit_price=lambda x: x["Unit Price Net"].apply(convert_excel_num_to_float),
                                    quantity=lambda x: x["Quantity"].apply(float),
                                    full_price=lambda x: x["unit_price"] * x["quantity"],
                                    discount=lambda x: 0,
                                    dollar_vol=lambda x: x["unit_price"] * x["quantity"],
                                    #Delivery=lambda x:x["Delivery"],w
                                    assigned_rep=None) \
    .pipe(correct_door_names, ref_dataset=il_name_corrections, state="IL")
    return df


il_door_name_remap_dict = il_name_corrections.to_dict()

def add_Delivery_il(door):
    if door in ca_door_name_remap_dict.keys():
        Delivery = il_door_name_remap_dict["Delivery"][door]    
    return Delivery



il_data_with_date = il_data.pipe(proc_il_data) \
    .query("use_date >= Timestamp('2022-01-01 00:00:00')") \
    .pipe(filter_pre_april_2022)

il_updated_with_date = il_updated.pipe(proc_il_data) \
    .pipe(filter_post_april_2022)

# print(il_updated_with_date)