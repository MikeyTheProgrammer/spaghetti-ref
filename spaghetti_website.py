# This file contains functions that are visible and a part of the website front-end (client side)
# it contains functions that handles the output method.
# and have the main running flow function of the page.

import streamlit as st
import pandas as pd
import datetime
from Utils.mail_sender import \
    send_mail
import os
import website_functions as wf
from io import BytesIO
import base64


SPAGHETTI = 'Spaghetti'
DAY_BACK_LIMIT = 90
AMOUNT_LEVELS_LIMIT = 10
AMOUNT_NUMBERS_LIMIT = 1000
EXCEL_OUTPUT_NAME_LENGTH_LIMIT = 30
# INPUT_OPTIONS = ('excel') #, 'list')
EPI_NUMBER_COLL = 'מספר אפידימיולוגי'
ID_NUMBER_COLL = 'תעודת זהות'
PHONE_NUMBER_COLL = 'מספר טלפון'
EXCEL_COLUMNS = [EPI_NUMBER_COLL, ID_NUMBER_COLL, PHONE_NUMBER_COLL]

MAIL = 'Mail (only MOH mail is currently supported)'
EXCEL = 'Excel'
OUTPUT_METHODS = (MAIL, EXCEL)

#MAIL_SENDER = 'roy.weber@MOH.GOV.IL'
MAIL_SENDER = 'ori.silberstein@MOH.GOV.IL'
FILE_DIR = r'/mnt/spaghetti'
SPAGHETTI_EXCELS_DIR = FILE_DIR+'/spaghetti_excels'
template_file = 'template.xlsx'
MALTEGO_FILES = r'/mnt/spaghetti/maltego'
maltego_zip_file = 'maltego_utils'

DEFAULT_MAIL = 'YourMail@moh.gov.il'
DEFAULT_NAME = 'For_'
DEFAULT_UP = 2
DEFAULT_DOWN = 10
DEFAULT_DAYS_BACK = 60

RUNNING_TIMES = 0

page_title = 'Spaghetti Interface'
page_description = 'Tracing exposures and close contacts' #'אתר להרצת ספגטי - אלגוריתם למציאת חשיפות ומגעים של מאומתים על מספר ימים אחורה ומספר דורות'
date_description_input = 'How many days to run back?' #'כמה ימים אחורה לבדוק?'
date_description_output = 'Dates to run on:' #'בודק בימים הבאים:'
levels_up_description = 'How many levels to go up?' #'כמה רמות למעלה לבדוק? (מי יכל להדביק אותי)'
levels_down_description = 'How many levels to go down?' #'כמה רמות למטה לבדוק? ( מי יכלתי להדביק )'

# This function adds a title and content to the website.
def page_start():
    st.title(page_title)
    st.write(page_description)

# This function sets the date-range.
def dates_options():
    days_back = st.number_input(date_description_input, min_value=1, max_value=DAY_BACK_LIMIT,
                                value=DEFAULT_DAYS_BACK)
    date_from = datetime.datetime.now().date() - datetime.timedelta(days_back)
    date_to = datetime.datetime.now().date()
    st.write(date_description_output, date_from, '->', date_to)
    return days_back

# This function sets how many levels up and down you go.
def levels_options():
    level_up = st.number_input(levels_up_description, min_value=1, max_value=AMOUNT_LEVELS_LIMIT, value=DEFAULT_UP)
    level_down = st.number_input(levels_down_description, min_value=1, max_value=AMOUNT_LEVELS_LIMIT,
                                 value=DEFAULT_DOWN)
    st.write('Levels up: ', level_up, '\tLevels down: ', level_down)
    return level_down, level_up

# This function sets the output method and contains two methods, Excel or Mail.
def output_settings():
    output_method = st.radio("How would you like to get the results?", OUTPUT_METHODS,
    key=str(RUNNING_TIMES))
    receiver = ''
    validation = True
    error = ''
    if output_method == MAIL:
        receiver = st.text_input('Enter email receiver (moh mail)', DEFAULT_MAIL)
        receiver = receiver.lower()
        if not receiver.endswith('@moh.gov.il'):
            validation = False
            error = 'Please enter a valid moh.gov.il mail'

    excel_name = st.text_input('Enter name for output excel', DEFAULT_NAME)
    if len(excel_name) > EXCEL_OUTPUT_NAME_LENGTH_LIMIT:
        validation = False
        error = 'Name of excel output can\'t have more than 30 characters'
    return validation, receiver, excel_name, error, output_method

# This function checks if the input is according to the template.
def check_df_input(input_df):
    sum_good_cols = 0
    for column in EXCEL_COLUMNS:
        if column in list(input_df.columns):
            sum_good_cols += len(input_df[column].dropna())
    run_excel = AMOUNT_NUMBERS_LIMIT > sum_good_cols > 0
    return run_excel, sum_good_cols

# This function sends the excel to the email.
def send_and_save_to_mail(spaghetti_df, receiver, excel_name):
    saved_and_sent = True
    try:
        subject_spaghetti = SPAGHETTI + '_' + excel_name + '_' + str(datetime.datetime.now().date())
        excel_dir = os.path.join(SPAGHETTI_EXCELS_DIR, subject_spaghetti + '.xlsx')
        print('Saving excel at', excel_dir)
        spaghetti_df.to_excel(excel_dir,index=False)
        print('Sending mail', excel_dir)
        send_mail(MAIL_SENDER, receiver, subject_spaghetti, excel_dir)
        print('Mail is sent!', excel_dir)
        os.remove(excel_dir)
    except Exception as e:
        st.write(e)
        print(e)
        saved_and_sent = False
    return saved_and_sent

# This function handles the output of the spaghetti
def handle_output(spaghetti_df, output_method, receiver, excel_name):
    st.write('Some of your new spaghetti data table:',
             spaghetti_df[list(spaghetti_df.columns)[:3]].head())
    st.success('Bon appetite :)')
    if output_method == MAIL:
        saved_and_sent = send_and_save_to_mail(spaghetti_df, receiver, excel_name)
        if saved_and_sent:
            st.write('saved!')

    elif output_method == EXCEL:
        st.markdown(wf.download_link_from_df(spaghetti_df, excel_name),
                    unsafe_allow_html=True)
    
# This function is the main spaghetti function, it lets you upload the template file, checks if you can run the system
# returns error message if needed and if not, the it runs the all system.
def input_options(days_back, level_down, level_up, receiver, excel_name, output_method):
    global RUNNING_TIMES

    is_running_active = wf.get_running_status()
    finished = False

    if is_running_active:
        st.error('wait, another person is spaghetting right now')
        return finished

    download_template_file()
    #st.write('enter excel! excel should have columns ' + EPI_NUMBER_COLL + ' or ' + ID_NUMBER_COLL, ' or both')
    positive_only = st.checkbox('Positive only')
    if positive_only:
        st.write('positive only')
    uploaded_file = st.file_uploader("Choose a file")

    if uploaded_file:
        try:
            input_df = pd.read_excel(uploaded_file, dtype={PHONE_NUMBER_COLL: str})
            st.write('some of your data:', input_df[list(input_df.columns)[:3]].head())
            empty_button = st.empty()
            button = empty_button.button('run spaghetti', key=str(RUNNING_TIMES))

            if button:

                is_running_active = wf.get_running_status()
                if is_running_active:
                    st.error('wait, another person is spaghetting right now')
                    return finished

                # wf.change_running_status(1)
                wf.increase_run_times()
                RUNNING_TIMES += 1
                empty_button.empty()

                valid_input, num_inputs = check_df_input(input_df)
                if not valid_input:
                    st.error(
                        f'could not run excel. pay attention for name of columns and for amount of numbers (less '
                        f'then {AMOUNT_NUMBERS_LIMIT})')

                else:
                    st.write(f'running on {num_inputs} ids and/or epi numbers')

                    gif_place_holder = st.empty()
                    with st.spinner('making spaghetti..'):
                        wf.gif('spaghetti_making.gif', gif_place_holder)
                        error, spaghetti_df = wf.run_spaghetti(input_df, days_back, level_down, level_up)

                    gif_place_holder.empty()
                    st.write(spaghetti_df.columns)

                    if positive_only:
                        spaghetti_df = spaghetti_df.loc[spaghetti_df['contact_was_ever_positive']]

                    if error != '':
                        st.error(error)
                    else:
                        handle_output(spaghetti_df, output_method, receiver, excel_name)
                        finished = True

        except Exception as e:
            st.error('check if you entered excel file')
            st.error(e)
            print(e)

    wf.change_running_status(0)
    return finished

# This function lets you download the template excel.
def download_template_file():
    dir_excel = os.path.join(FILE_DIR,template_file)
    df_template = pd.read_excel(dir_excel)
    st.subheader('Template input file')
    st.write('Fill the template and upload')
    st.markdown(wf.download_link_from_df(df_template, 'template', description='Input Template'),
                unsafe_allow_html=True)

# This function lets you download the maltego zip file (with instructions and config)
def download_maltego_zip():
    st.subheader('Using Maltego')
    st.write('Download the file and follow the instructions')
    try:
        zip_file_dir = os.path.join(MALTEGO_FILES, maltego_zip_file)
        st.markdown(wf.download_zip_file(zip_file_dir,maltego_zip_file),
                    unsafe_allow_html=True)
    except Exception as e:
        st.write(e)

# This function tells you if the spaghetti is finished.
def end_page(is_finished):
    #download_template_file()
    if is_finished:
        gif_place_holder = st.empty()
        wf.gif("movie_spaghetti_gif.gif", gif_place_holder)
    download_maltego_zip()

# This function contains and executes the running flow of the website
def running_flow():
    page_start()
    days_back = dates_options()
    level_down, level_up = levels_options()
    validation, receiver, excel_name, error, output_method = output_settings()
    is_finished = False
    if validation:
        is_finished = input_options(days_back, level_down, level_up, receiver, excel_name, output_method)
    else:
        st.error(error)
    end_page(is_finished)




if __name__ == '__main__':
    running_flow()


# elif creating_method == 'list':
#     epies_number = st.text_area('enter list of epies with , between 340598,349508,34098..', key='3')
#     idies = st.text_area('enter list of idies with , between each 340598,349508,34098..', key='4')
#     if st.button('run spagetti'):
#         print(epies_number.split(','))
#         print(idies.split(','))
#         epies_number = epies_number.split(',')
#         epies_number = [number.strip() for number in epies_number if number.strip().isdigit()]
#         idies = idies.split(',')
#         idies = [number.strip() for number in idies if number.strip().isdigit()]
#         users_df = pd.concat([pd.DataFrame(epies_number, columns=[EPI_NUMBER_COLL]), pd.DataFrame(idies, columns=[ID_NUMBER_COLL])])
#         run_spaghetti_flag = check_df_input(users_df)
#         if not run_spaghetti_flag:
#             st.error('could not run excel. pay attention for amount of numbers (less then 50)')
#         else:
#             run_spaghetti(users_df,days_back,level_down,level_up,receiver,excel_name)

    # st.markdown("""
    # <style>
    # input{
    # unicode-bidi:bidi-override;
    # direction: RTL;
    # }
    # text{
    # unicode-bidi:bidi-override;
    # direction: RTL;
    # }
    # </style>
    # """,unsafe_allow_html = True)