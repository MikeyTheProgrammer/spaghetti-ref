# This file contains functions that are essential for the running of the website
# it contains functions that relates to the running status, functions for downloading files
# and the main running function that kickstart the system.

import json
#import streamlit as st
from io import BytesIO
import pandas as pd
import base64
from spaghetti import Spaghetti
EPI_NUMBER_COLL = 'מספר אפידימיולוגי'
ID_NUMBER_COLL = 'תעודת זהות'
PHONE_NUMBER_COLL = 'מספר טלפון'

# This function checks if there is a spaghetti running currently active.
def get_running_status():
    with open('logs.json') as f:
        logs = json.load(f)
    return logs['running_active']

# This function updates the file logs.json to the current status.
def change_running_status(status):
    with open('logs.json') as f:
        logs = json.load(f)
    logs['running_active'] = status
    with open('logs.json', 'w') as f2:
        json.dump(logs, f2)

# This function updates the log and adds a plus 1 to the run times.
def increase_run_times():
    with open('logs.json') as f:
        logs = json.load(f)
    logs['num_runs'] += 1
    with open('logs.json', 'w') as f2:
        json.dump(logs, f2)

# This function creates a download link of the excel file created by spaghetti.
def download_link_from_df(df, excel_name, description='Results'):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, sheet_name='Sheet1', index=False)
    writer.save()
    processed_data = output.getvalue()
    b64 = base64.b64encode(processed_data)
    return f'<a href="data:application/octet-stream;base64,{b64.decode()}" download="{excel_name}.xlsx"> Download ' \
           f'{description}</a>'

# This function creates a zip file with the maltego configurations.
def download_zip_file(file_dir,file_name):
    with open(file_dir+'.zip','rb') as f:
        bytes = f.read()
        b64 = base64.b64encode(bytes).decode()
        href = f'<a href="data:file/zip;base64,{b64}" download=\'{file_name}.zip\'>\
                Click to download maltego config</a>'
    return href

# This function allows the developer to create a gif using base64.
def gif(dir,gif_place_holder):
    file_ = open(dir, "rb")
    contents = file_.read()
    data_url = base64.b64encode(contents).decode("utf-8")
    file_.close()
    gif_place_holder.markdown(
        f'<img src="data:image/gif;base64,{data_url}" alt="cat gif">',
        unsafe_allow_html=True)

# This function cooks you spaghetti (Runs the system)
def run_spaghetti(users_df, days_back, level_down, level_up):
    try:
        spaghetti_df = Spaghetti.run_on_dataframe(users_df,
                                                  fname='',
                                                  directory='',
                                                  epi_number_col=EPI_NUMBER_COLL,
                                                  id_number_col=ID_NUMBER_COLL,
                                                  phone_number_col=PHONE_NUMBER_COLL,
                                                  levels_up=level_up,
                                                  levels_down=level_down,
                                                  days_back=days_back,
                                                  save=False,
                                                  summarize=True,
                                                  to_send_email=False,
                                                  sender='',
                                                  receiver=''
                                                  )

        return '', spaghetti_df
    except Exception as e:
        print(e)
        return e, None
