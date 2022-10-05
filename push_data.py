# PUSHES DATA INTO A GOOGLE SHEET

from datetime import datetime as dt
import numpy as np
#### GET MY GOOGLE SHEET DATA
import sys
import subprocess
import pandas as pd
from pre_combined import *
# combined_with_manual_sku_map

scope =  ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/drive.file', "https://www.googleapis.com/auth/drive","https://www.googleapis.com/auth/drive"]

import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials

# KEEP CREDS.JSON, AS IT SERVES THE AUTH KEY FOR THIS PROJECT
creds = ServiceAccountCredentials.from_json_keyfile_name("creds.json", scope)
client = gspread.authorize(creds)
sheet = client.open("CANN SALES").worksheet("Sheet1")
sheet.clear()
set_with_dataframe(worksheet=sheet, dataframe=combined_with_manual_sku_map, include_index=False,
include_column_header=True, resize=True)

success_messsage = 'Import to Drive & Tableau Completed!'


# data = sheet.get_all_records()
# print(data)

#### UNCOMMENT ABOVE ^^^^ 

# IF YOU JUST WANT SAMPLE DATA, UNOMMENT BELOW:
# data = [{'Timestamp': '9/8/2022 0:33:02', 'How Do You Feel?': 4}, {'Timestamp': '9/8/2022 0:51:30', 'How Do You Feel?': 5}, {'Timestamp': '9/8/2022 10:04:27', 'How Do You Feel?': 3}, {'Timestamp': '9/9/2022 7:40:20', 'How Do You Feel?': 3}, {'Timestamp': '9/9/2022 11:45:42', 'How Do You Feel?': 3}, {'Timestamp': '9/9/2022 13:56:57', 'How Do You Feel?': 2}, {'Timestamp': '9/9/2022 19:03:37', 'How Do You Feel?': 4}, {'Timestamp': '9/9/2022 21:31:01', 'How Do You Feel?': 3}, {'Timestamp': '9/10/2022 11:44:07', 'How Do You Feel?': 3}]
# END OF SAMPLE 


# y_axis = [line['How Do You Feel?'] for line in data]
# x_axis = [dt.strptime(line["Timestamp"], "%m/%d/%Y %H:%M:%S") for line in data]
# data = [{'timestamp':x_axin, 'mood':y_axin} for y_axin, x_axin in zip(y_axis, x_axis)]  


# workbooker = client.open("How do you feel")
# sheet = workbooker.worksheet('Gym Records')
# fitness_data = sheet.get_all_records()
# dates_visited_gym = [dt.strptime(line['Date'], "%m/%d/%Y") for line in fitness_data]
# data = pd.DataFrame(data)
# daily_data = data.groupby(by=[pd.Grouper(key='timestamp', axis=0, freq='D')]).mean()
# daily_data.index
# gym_df = pd.DataFrame(dates_visited_gym, index=dates_visited_gym)
# gym_df[0] = 1
# gym_df.index = pd.to_datetime(gym_df.index)
# gym_df.index = gym_df.index.rename('timestamp')
# daily_data = daily_data.merge(gym_df, on='timestamp', how='left').fillna(0)
# daily_data.columns = ['mood', 'gym_visit']

# daily_data['timestamp-1'] = daily_data.index - pd.offsets.DateOffset(days=1)
# daily_data['timestamp-2'] = daily_data.index - pd.offsets.DateOffset(days=2)
# daily_data['timestamp-3'] = daily_data.index - pd.offsets.DateOffset(days=3)
