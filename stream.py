import streamlit as st
from io import BytesIO
import subprocess
import sys

# CHECK DEPENDENCIES:
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 
# 'gspread_dataframe'])

st.session_state['processed'] = True



from pyxlsb import open_workbook as open_xlsb


# UNCOMMENT BELOW TO OPEN FULL PREPROCESS

# def load_data():
#     st.session_state['processed'] = False
#     options = c.multiselect(
#     'View Raw Databases',
#     ['CA', 'NV', 'IL', 'MA'])
#     with st.expander("Open to View Raw Databases"):
#         for option in options:
#             if option == "CA":
#                 from pre_ca import ca_to_use
#                 st.write(ca_to_use)
#             if option == "IL":
#                 from pre_il import il_updated_with_date
#                 st.write(il_updated_with_date)
#             if option == "NV":
#                 from pre_nv import nv_updated_with_date
#                 st.write(nv_updated_with_date)        
#             if option == "MA":
#                 from pre_ma import ma_updated_with_date
#                 st.write(ma_updated_with_date)


def to_excel(df):
    import pandas as pd
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    format1 = workbook.add_format({'num_format': '0.00'}) 
    worksheet.set_column('A:A', None, format1)  
    writer.save()
    processed_data = output.getvalue()
    return processed_data

def google_it():
    from push_data import success_messsage
    st.write(success_messsage)
    st.balloons()



def process():
    with st.spinner(text="Being Processed... View Prints in Console Log"):
        from pre_combined import combined_with_manual_sku_map
        st.session_state['processed'] = True
        return combined_with_manual_sku_map

import pandas as pd
url = 'https://kiva.encompass8.com/API?APICommand=ReportView&ReportName=All%20Sales&ReportID=13708091&BaseQuery=Sales&Action=Data&ReportIsEdit=True&Format=WebQuery&EncompassID=Kiva&QuickKey=1569376e7016fd1e065aafba93521b27&Parameters=F:ColumnValues~V:Company%5EDate%5EMonths~O:E|F:FieldValues~V:%24Vol~O:E|F:Period~V:ThisYear~O:E|F:YearInt~V:1~O:E|F:CloseDay~V:4~O:E|F:ChainID~V:78%5E5~O:NE|F:ParentChain~V:GrassDoor%5EMedMen~O:NE&'
encompass = pd.read_html(url, header=[0])
from push_data import push_data
encompass_df = encompass[0]
encompass_df = encompass_df.reset_index(drop=True)

# encompass_df.index = encompass[0].iloc[0]
st.write(encompass_df)

d = st.container()
d.write('SOCALI TABLEAU DATA ETL PREPROCESSOR')
if d.button("Push Encompass Data to Sales Tracker"):

    push_data(encompass_df, filepath="Cann - Q4'22 Rep Daily Sales Tracker")
    encompass_csv = to_excel(encompass_df)
    st.download_button('Download Encompass Report',encompass_csv, file_name="encompass_data.csv")
    st.markdown('### Successfully pushed data to [Sales Tracker](https://docs.google.com/spreadsheets/d/1a8xoFcWd_a-Q1dkL06eUTO-JA4y9hO3wPgNyGQYBIAI/edit#gid=711100757)')
else:
    pass


# UNCOMMENT BELOW FOR THE REST OF THE PREPROCESSOR

# c = st.container()
# c.write('SOCALI TABLEAU DATA ETL PREPROCESSOR')
# if c.button("Load Presaved Data"):
#     load_data()
#     st.session_state['load_data'] = True
# else:
#     load_data()



# if st.button("Process"):
#     combined_with_manual_sku_map = process()


# while st.session_state['processed'] == True:
#     col1, col2 = st.columns(2)
#     df_xlsx = to_excel(combined_with_manual_sku_map)

#     col1, col2 = st.columns(2)
#     col1.download_button('📥 Download', df_xlsx, file_name='CANN_SALES.xlsx')
#     col2.button('Push to Tableau', on_click=google_it)


## st.write(st.session_state)