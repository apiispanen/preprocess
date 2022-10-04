import pandas as pd
import numpy as np

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)

print("Reading Files")
# Read Data from Each State
ca_data = pd.read_csv("data/ca.csv")
il_data = pd.read_csv("data/il.csv")  # Leaftrade data
ma_data = pd.read_csv("data/ma.csv")
nv_data = pd.read_csv("data/nv.csv")



door_to_rep_cols_to_drop = ["Unnamed: 6", "Unnamed: 7" ,"Unnamed: 8", "Unnamed: 9"]
door_to_acct_to_rep = pd.read_csv("data/door_acct_rep_map.csv").drop(door_to_rep_cols_to_drop, axis=1)

sku_map = pd.read_csv("data/sku_map.csv")[["Product", "SKU Number", "Distro", "Type", "Format", "Packout", "Sample", "Flavor", "Rename"]].dropna(subset=["Product"])

# CA Account Mapping
ca_acct_mapping = pd.read_csv("ca_account_mapping_q1_2022.csv").rename(columns={"Supplier / Chain / Customer": "account"})
ca_unclaimed_doors = pd.read_csv("corrections_from_cann/unclaimed_ca_doors_from_javi.csv")


# Updated Data
ca_updated = pd.read_csv("data/ca_updated.csv")
ma_updated = pd.read_csv("data/ma_updated.csv")
il_updated = pd.read_csv("data/il_updated.csv")
nv_updated = pd.read_csv("data/nv_updated.csv")

ca_updated

print("Done Reading Files")
# Name Corrections
nv_name_corrections = pd.read_csv("corrections_from_cann/nv_name_corrections-HL-New.csv").set_index("Door Name in System")
ma_name_corrections = pd.read_csv("corrections_from_cann/ma_name_corrections-HL-New.csv").set_index("Door Name in System")
ca_name_corrections = pd.read_csv("corrections_from_cann/ca_name_corrections-HL-New.csv").set_index("door")
#ca_name_corrections
ca_name_corrections

# Function written to apply flavour to family mapping

def apply_flavor_to_fam_mapping(x):
    if x in flavor_to_fam_mapping.keys():
        return flavor_to_fam_mapping[x]
    else:
        return "No Fam"

def apply_Delivery_mapping(x):
    if x in Delivery1.keys():
        return Delivery1[x]
    else:
        return "No"

# Parsing Functions
def convert_excel_num_to_float(x):
    if isinstance(x, int):
        return float(x)
    elif isinstance(x, float):
        return x
    else:
        is_negative = ("(" in x) | ("-" in x)
        num = float(x.replace("-", "").replace("(", '').replace(")", '').replace(",", '').strip('$'))
        return -num if is_negative else num

## This should really be a regex
def parse_pack_size(x):
    if ("4-Pack" in x) | ("4pk" in x) | ("4 Pack" in x) | ("4-pack" in x) | ("Pack of 4" in x):
        return 4
    elif ("6-Pack" in x) | ("6pk" in x) | ("6 Pack" in x) | ("6-pack" in x) | ("Pack of 6" in x):
        return 6
    elif ("Single" in x) | ("SINGLES" in x):
        return 1
    elif ("3pk" in x):
        return 3
    elif ("10pk" in x):
        return 10
    elif ("8pk" in x):
        return 8
    elif ("Bottle" in x):
        return 1
    elif ("Space Crystals" in x):
        return 1
    else:
        return 0

def parse_container_size(x):
    if ("Hi Boy" in x) | ("12oz" in x) |("hi boy" in x)|("Hi-Boys" in x):
        return "12oz"
    elif ("8oz" in x):
        return "8oz"
    elif "Roadie" in x:
        return "Sachet"
    elif "Variety" in x: # If its variety and no other size can be determined, its 8oz
        return "8oz"
    elif "Bottle" in x:
        return "Bottle"
    elif "Space Crystals" in x:
        return "Crystals"
    else:
        return "8oz"

def parse_flavor(x):
    for flavor in flavor_to_abbrev.keys():
        if flavor in x:
            return flavor_to_abbrev[flavor]
    return "Flavor Not Found"

def parse_Delivery(x):
    for d in Delivery.keys():
        if d in x:
            return Delivery[d]
    return "No"

def determine_sku(x):
    pack_size = parse_pack_size(x)
    container_size = parse_container_size(x)
    flavor = parse_flavor(x)
    sku_base = flavor + "-" + container_size + "-" + str(pack_size)
    return sku_base

def is_lite(x):
    return "Lite" in x

def is_sample(x):
    return ("SAM" in x) | ("staff" in x)

def is_promotion(x):
    return ("PRO" in x) | ("promotion" in x)


def assign_rep(x, state, account):
    if isinstance(x, str):
        return x
    else:
        if account in acct_owners.keys():
            return acct_owners[account]
        elif state in state_defaults.keys():
            return state_defaults[state]
        else:
            "Not Mapped"


# Only CA has door to rep mappings
ca_cols_to_drop = ["Unnamed: 15",
                    "Unnamed: 16",
                    "Unnamed: 17",
                    "Unnamed: 18",
                    "Unnamed: 24",
                    "Unnamed: 33",
                    "Door Name",
                    "Account Name",
                    "Segment",
                    "Geography",
                    "Top 15?",
                    "Month",
                    "Quarter"]

def remove_dtc_accounts(df):
    dtc_accounts = ["GrassDoor"]
    return df[df["account"].apply(lambda x: x not in dtc_accounts)]

ca_data_with_date = ca_data.drop(columns=ca_cols_to_drop) \
    .assign(use_date=lambda x: pd.to_datetime(x["Date"]),
            state="CA",
            door=lambda x: x["Company"],
            account=lambda x: x["Chain"],
            ext_doc_id=lambda x: x["Invoice Num"],
            ext_doc_type="Invoice",
            product_name=lambda x: x["Product Name"],
            promo_name=lambda x: x["Promotion Name"],
            unit_price=lambda x: x["Unit Price"].apply(convert_excel_num_to_float),
            quantity=lambda x: x["Units"].apply(convert_excel_num_to_float),
            full_price=lambda x: x["Full Price"].apply(convert_excel_num_to_float),
            discount=lambda x: x["Discount"].apply(convert_excel_num_to_float),
            dollar_vol=lambda x: x["full_price"] - x["discount"]) \
    .pipe(remove_dtc_accounts) \
    .query("use_date < Timestamp('2022-04-01 00:00:00')")


ca_updated_with_date = ca_updated \
    .assign(use_date=lambda x: pd.to_datetime(x["Date"]),
            state="CA",
            door=lambda x: x["Company"],
            account=lambda x: x["Chain"],
            ext_doc_id=lambda x: x["Invoice Num"],
            ext_doc_type="Invoice",
            product_name=lambda x: x["Product Name"],
            promo_name=lambda x: x["Promotion Name"],
            unit_price=lambda x: x["Unit Price"].apply(convert_excel_num_to_float),
            quantity=lambda x: x["Units"].apply(convert_excel_num_to_float),
            full_price=lambda x: x["Full Price"].apply(convert_excel_num_to_float),
            discount=lambda x: x["Discount"].apply(convert_excel_num_to_float),
            dollar_vol=lambda x: x["full_price"] - x["discount"]) \
    .pipe(remove_dtc_accounts) \
    .query("use_date >= Timestamp('2022-04-01 00:00:00')")

ca_columns = ["use_date", "state", "door", "account", "ext_doc_id", "ext_doc_type", "product_name", "promo_name", "unit_price", "quantity", "full_price", "discount", "dollar_vol"]
# Using Concatenate function to merging the two updated data frame and the other dataframe

ca_to_use = pd.concat([ca_data_with_date[ca_columns], ca_updated_with_date[ca_columns]])   # Concatenate- merged data

## Create account adjustment mapping


# This may break the case of acquisitions where a door keeps the same "door" name but changes account

# Select only the most recent account

ca_door_to_acct_map = ca_to_use.sort_values("use_date", ascending=False).groupby("door").head(1)[["door", "account"]]  # Created a dictionary
ca_door_to_acct_map
ca_remap_dict = ca_door_to_acct_map.set_index("door").to_dict()["account"]   # Account is the key 

ca_door_name_remap_dict = ca_name_corrections.to_dict()       #CA_NAme_Corrections is a 

ca_unclaimed_door_dict = ca_unclaimed_doors.set_index("door").to_dict()["Current Rep Owner"] # ambiguous- dont know why it is created

def update_ca_door(door):                                                      # 
    #Check if door name needs remapping
    if door in ca_door_name_remap_dict.keys():
        door = ca_door_name_remap_dict["updated_door_name"][door]
    return door

def update_ca_acct(door):
    # Remap all accounts
    account=ca_remap_dict[door]
    return account

rep_dict = ca_acct_mapping.set_index("account").to_dict()["Rep"]
def assign_ca_rep(account, door):
    if account in rep_dict.keys():
        return rep_dict[account]
    elif door in rep_dict.keys():
        return rep_dict[door]
    elif door in ca_unclaimed_door_dict.keys():
        return ca_unclaimed_door_dict[door]
    else:
        return "Not In Mapping"

def add_Delivery(door):
    if door in ca_door_name_remap_dict.keys():
        Delivery = ca_door_name_remap_dict["Delivery"][door]    
    return Delivery
    
ca_to_use["door"] = ca_to_use["door"].apply(update_ca_door)   # apply checks for each row of merged file ex-d1
ca_to_use["account"] = ca_to_use["door"].apply(update_ca_acct)

ca_to_use["assigned_rep"] = ca_to_use.apply(lambda row: assign_ca_rep(row["account"], row["door"]), axis=1)

ca_to_use.groupby("assigned_rep").agg({"dollar_vol": "sum"})
print(ca_to_use)


