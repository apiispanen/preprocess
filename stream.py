import streamlit as st
from io import BytesIO
import subprocess
import sys

# CHECK DEPENDENCIES:
# subprocess.check_call([sys.executable, '-m', 'pip', 'install', 
# 'gspread_dataframe'])

st.session_state['processed'] = True

from pyxlsb import open_workbook as open_xlsb

def load_data():
    st.session_state['processed'] = False
    options = c.multiselect(
    'View Raw Databases',
    ['CA', 'NV', 'IL', 'MA'])
    with st.expander("Open to View Raw Databases"):
        for option in options:
            if option == "CA":
                from pre_ca import ca_to_use
                st.write(ca_to_use)
            if option == "IL":
                from pre_il import il_updated_with_date
                st.write(il_updated_with_date)
            if option == "NV":
                from pre_nv import nv_updated_with_date
                st.write(nv_updated_with_date)        
            if option == "MA":
                from pre_ma import ma_updated_with_date
                st.write(ma_updated_with_date)


c = st.container()
c.write('SOCALI TABLEAU DATA ETL PREPROCESSOR')
if c.button("Load Data"):
    load_data()
    st.session_state['load_data'] = True
else:
    load_data()


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


if st.button("Process"):
    combined_with_manual_sku_map = process()


while st.session_state['processed'] == True:
    col1, col2 = st.columns(2)
    df_xlsx = to_excel(combined_with_manual_sku_map)

    col1, col2 = st.columns(2)
    col1.download_button('📥 Download', df_xlsx, file_name='CANN_SALES.xlsx')
    col2.button('Push to Tableau', on_click=google_it)


# st.write(st.session_state)