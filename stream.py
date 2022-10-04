import streamlit as st

st.write('test')

options = st.multiselect(
    'Databases',
    ['CA', 'NV', 'IL', 'MA'])

def process():
    from pre_combined import combined_with_manual_sku_map
    st.download_button('Download', combined_with_manual_sku_map.to_csv().encode('utf-8'))


for option in options:
    if option == "CA":
        from pre_ca import *
        st.write(ca_to_use)
    if option == "IL":
        from pre_il import *
        st.write(il_updated_with_date)
    if option == "NV":
        from pre_nv import *
        st.write(nv_updated_with_date)        
    if option == "MA":
        from pre_ma import *
        st.write(ma_updated_with_date)
    



st.write('You selected:', options)
st.button("Process",on_click=process)