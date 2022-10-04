# Reference Tables
from pandas import Series


flavor_to_abbrev = {"Blood Orange Cardamom": "BOC",
                    "Cranberry Sage": "CS",
                    "Grapefruit Rosemary": "GR",
                    "Grapefruit": "GR",
                    "Lemon Lavender": "LL",
                    "Ginger Lemongrass": "GLG",
                    "Pineapple": "PJ",
                    "Yuzu Elderflower": "YE",
                    "Passion Peach Mate": "PPM",
                    "Variety": "Variety",
                    "Honeydew Mint": "HM",
                    "Lime Basil": "LB",
                    "Blue Rhuberry": "BR",
                    "Variety": "Variety",
                    "GR": "GR",
                    "LL": "LL",
                    "BOC": "BOC",
                    "PJ": "PJ"}

  # Trying to create changes in the delivery yes and no columns- created dictionary

acct_owners = {"Eaze": "Elyse",
                "MedMen": "Jen",
                "CuraLeaf": "Gia",
                "Ascend": "Elyse",
                "GTI": "Gia"}

state_defaults = {"CA": "Not Mapped",
                "IL": "Latifah",
                "NV": "Gia",
                "RI": "Not Mapped",
                "MA": "Joe"}

flavor_to_fam_mapping = \
    {"BOC": "Core",
    "LL": "Core",
    "GR": "Core",
    "CS": "Seasonal",
    "PJ": "Seasonal",
    "GLG": "Seasonal",
    "HM": "Lite",
    "YE": "Reserve",
    "LB": "Custom",
    "PPM": "Custom",
    "BR": "Custom",
    "Variety": "Variety"}

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
        try:
            if account in acct_owners.keys():
                return acct_owners[account]
            elif state in state_defaults.keys():
                return state_defaults[state]
            else:
                return "Not Mapped"
        except:
            print("NOT MAPPED: ", [name for name in account])            
            return "Not Mapped"
            

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
