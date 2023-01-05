# this file contains all of the queries required for the needed information.

from Utils.SQLConnector import \
    SQLConnector
import pandas as pd
import numpy as np
import datetime

BATCH_SIZE = 1000
MAX_CONTACT_DAYS_BACK = 14

#This function splits a list to parts of the size of batch size
def split(my_list, batch_size=BATCH_SIZE):
    all_splits = []
    for i in range(0, int(np.ceil(len(my_list) / batch_size))):
        batch = my_list[i * batch_size: (i + 1) * batch_size]
        if len(batch) != 0:
            all_splits.append(batch)
    return all_splits

#This function gets a list of ids and returns the ids in the list joined by "union all".
def build_select(ids_to_insert):
    select = ''

    for idx, id_num in enumerate(ids_to_insert):

        record = id_num
        if isinstance(record, str):
            record = "'" + record + "'"

        if idx == 0:
            select = select + f"""Select {record} as id"""
        else:
            select = select + f"""\nUnion all Select {record}"""

    return select

#this function is unused.
def list_to_query_str(my_list):

#This function gets the patient ids according to id numbers, epi numbers or phone numbers

def get_patient_ids(bi_conn: SQLConnector, id_numbers=[], epi_numbers=[], phone_numbers=[]):
    all_patient_ids = set()

    for current_epi_numbers in split(epi_numbers):

        my_select = build_select(current_epi_numbers)

        query = f"""
                select distinct
                        i.new_validation_case_number as epi_number
                        , i.patient_ID
                from ({my_select}) ids
                join dwh.Dim_Fact_Corona_Investigation_Model i
                on i.new_validation_case_number = ids.id
                """
        df = bi_conn.query_by_str(query)

        for _, row in df.iterrows():
            epi_number = row['epi_number']
            patient_id = row['patient_ID']
            if patient_id is not None and epi_number is not None:
                all_patient_ids.add(patient_id)

    for current_id_numbers in split(id_numbers):

        my_select = build_select(current_id_numbers)

        query = f"""
                select distinct
                    p.new_IdentityNumber as id_number
                    , p.patient_ID
                from ({my_select}) ids
                join dwh.Dim_Corona_Patient_Model p
                on p.new_IdentityNumber = ids.id
                """
        df = bi_conn.query_by_str(query)
        for _, row in df.iterrows():
            id_number = row['id_number']
            patient_id = row['patient_ID']
            if patient_id is not None and id_number is not None:
                all_patient_ids.add(patient_id)

    for current_phone_numbers in split(phone_numbers):
        my_select = build_select(current_phone_numbers)

        query = f"""
                select distinct
                    phones.id as telephone
                    , p.patient_ID
                from ({my_select}) phones
                join dwh.Dim_Corona_Patient_Model p
                on p.Telephone = phones.id or p.telephone2 = phones.id
                """
        df = bi_conn.query_by_str(query)
        for _, row in df.iterrows():
            phone_number = row['telephone']
            patient_id = row['patient_ID']
            if patient_id is not None and phone_number is not None:
                all_patient_ids.add(patient_id)

    return all_patient_ids

#this function finds the exposures.
def perform_exploration(patient_ids, bi_conn: SQLConnector, direction='up', days_back=MAX_CONTACT_DAYS_BACK):
    if direction == 'up':
        exposure_match = 'patient_ID'
        contact_match = 'cp.patient_id'

    else:
        exposure_match = 'ExposureTo_patient_ID'
        contact_match = 'p.patient_id'

    matches = set()

    for current_ids in split(patient_ids):

        my_select = build_select(current_ids)

        exposure_query = f"""
        select distinct
                 e.ExposureTo_patient_ID as father_patient_id
                ,e.patient_ID as son_patient_id
                ,e.new_ExposureDate as contact_date

        from ({my_select}) ids

        join dwh.Fact_Corona_Exposure_Model e
        on e.{exposure_match} = ids.id
        and e.new_ExposureDate >= cast(getdate()-{days_back} as date)
        """

        contact_query = f"""
        select 
            p.patient_ID as father_patient_id
            , cp.patient_ID as son_patient_id
            , isnull(c.new_contactdate, c.CreatedOn) as contact_date

        from dwh.Dim_Fact_Corona_Investigation_Model i

        inner join stg.Filterednew_ContactTable c
        on i.investigation_ID = isnull(c.new_ncoronavinvestigation, c.new_ncovinvestigation)
        and i.investigation_ID is not null
        and i.first_positive_result_test_date is not null
        and i.new_validation_case_number is not null
        and i.new_IdentityNumber is not null
        and c.new_contactidnumber is not null

        inner join dwh.Dim_Corona_Patient_Model p
        on p.patient_ID = i.patient_ID
        and p.patient_ID is not null

        inner join dwh.Dim_Corona_Patient_Model cp
        on cp.new_IdentityNumber = c.new_contactidnumber
        and cp.patient_ID is not null

        inner join ({my_select}
        ) ids
        on ids.id = {contact_match}

        where isnull(c.new_contactdate, c.CreatedOn) >= cast(getdate()-{days_back} as date)
        and (c.new_closenessname='מגע הדוק' or 
        (c.new_closenessname='אופציונאלי' and 
        (c.new_ParamLiveTogether='כן' or 
        isnull(p.investigatin_address, p.address) = isnull(cp.investigatin_address, cp.address))
        ))
        """

        contact_df = bi_conn.query_by_str(contact_query)
        exposure_df = bi_conn.query_by_str(exposure_query)

        for df in [contact_df, exposure_df]:
            for _, row in df.iterrows():
                father_patient_id = row['father_patient_id']
                son_patient_id = row['son_patient_id']
                contact_date = row['contact_date']

                if father_patient_id is None or son_patient_id is None or pd.isna(contact_date) \
                        or contact_date < datetime.datetime.today() - datetime.timedelta(days=days_back):
                    continue

                matches.add((father_patient_id,
                             son_patient_id,
                             contact_date))

    return matches


#This function maps the variants according to countries
def map_to_variant_name(val):
    # not_found = ['no monitored variant', 'run fail', 'full_sequence_result', 'A suspect', 'NULL', 'no result'
    #     , 'suspect', 'b.1, 0 - no variant', 'b.1.1.50 - no monitored variant', 'novel - not assign']

    val = val.lower()
    if 'a.23.1' in val:
        return 'uganda'
    elif 'c.37' in val:
        return 'chile'
    elif 'b.1.617' in val or 'b.1.618' in val:
        return 'india'
    elif 'b.1.1.7' in val:
        return 'uk'
    elif '484' in val:
        return '484'
    elif 'b.1.526' in val:
        return 'new york'
    elif 'israel' in val:
        return 'israel'
    elif 'b.1.351' in val:
        return 'sa'
    elif 'b.1.525' in val:
        return 'global'
    elif 'at.1' in val:
        return 'st. petersburg'
    elif 'p.1' in val:
        return 'manaus'
    elif 'b.1.429' in val:
        return 'california'
    elif 'ba.1' in val or 'B.1.1.529' in val:
        return 'omicron'
    # elif val in not_found:
    #     return 'no variant match found'
    else:
        return 'no variant match found'

#This function gets spaghetti's parameters from the sql databases
def get_patient_information(patient_ids, bi_conn: SQLConnector, xrm_conn: SQLConnector, anonymous_name=False):
    res_dict = {}

    for current_patient_ids in split(patient_ids):

        my_select = build_select(current_patient_ids)

        bi_query = f"""

                    with ids as (
                    {my_select}
                    )

                    ,full_sequence_res as (

                        select f1.id_number
                            ,p.patient_id as id
                            ,ROW_NUMBER() over (partition by f1.id_number order by f1.sequence_arrival_date  desc) as variant_number
                            ,f1.full_sequence_result_date
                            ,f1.full_sequence_result
                        from 
                        (
                            select id_number
                                ,full_sequence_result_date
                                ,sequence_arrival_date
                                , case 
                                        when seq.full_sequence_result in ('no result', 'run fail') then NULL 
                                        else seq.full_sequence_result end as full_sequence_result
                            from bi_corona.mrr.df_tests seq
                        ) f1
                        left join dwh.Dim_Corona_Patient_Model p
                        on p.new_IdentityNumber = f1.id_number
                        where f1.full_sequence_result is not null

                    )

                    ,last_take_dates as 
                    (
                        select
                            ids.id
                            , max(t.new_takedate_calc) as last_take_date
                        from ids
                        join dwh.Fact_Corona_Lab_Test_Model t
                        on t.patient_ID = ids.id
                        group by ids.id
                    )
                        select distinct 
                                p.patient_ID
                                , p.fullname as 'full_name'
                                , (case
                                    when i.new_validation_case_number is not null then cast(i.new_validation_case_number as varchar(20))
                                    else p.new_IdentityNumber
                                    end) as 'final_id'
                                , p.new_IdentityNumber as 'id_number'
                                , i.new_validation_case_number as 'epi_number'
                                , p.age as 'age'
                                , p.city_desc as 'city'
								,p.address
                                , (case
                                        when i.first_positive_result_test_date is not null then 1
                                        else 0
                                    end) as 'was_ever_positive'
                                , i.first_positive_result_test_date as 'confirmation_date'
                                , p.Telephone as 'phone'
                                , i.date_recovery as 'recovery_date'
                                , last_take_dates.last_take_date as 'last_test_sampling_date'
                                , cast(p.first_vaccination_date as datetime) as 'first_vac_date'
                                , cast(p.second_vaccination_date as datetime) as 'second_vac_date'
                                , cast(p.third_vaccination_date as datetime) as 'third_vac_date'
                        ,f1.full_sequence_result as 'variant'
                        ,f1.full_sequence_result_date as 'variant_date'
                        ,max(f2.visited_country) over(partition by f2.id_number order by f2.arrival_date desc) as 'visited_country'
                        ,max(f2.arrival_date) over(partition by f2.id_number order by f2.arrival_date desc) as 'arrival_date'
						,max(v.dose_ind) over(partition by v.Person_Id order by v.vaccination_date_time desc) as doses_amount
						,max(v.vaccination_date_time) over(partition by v.Person_Id order by v.vaccination_date_time desc) as last_dose_date
						,max(case when i_past.date_recovery is not null then 1 else 0 end) over(partition by i_past.patient_ID order by i_past.date_recovery desc) as is_return_sick
						,max(case when ser.patient_ID is not null then 1 else 0 end) over(partition by ser.patient_ID order by ser.new_takedate desc) as is_serology_protected
						,p.Telephone
						,p.telephone2
						,p.telephone3

                        from ids
                        join dwh.Dim_Corona_Patient_Model p
                        on p.patient_id = ids.id

                        left join dwh.Dim_Fact_Corona_Investigation_Model i
                        on p.patient_ID = i.patient_ID

                        left join dwh.Dim_Fact_Corona_Investigation_Model i_past
                        on p.patient_ID = i_past.patient_ID and i_past.date_recovery <= i.first_positive_result_test_date

                        --left join dwh.Fact_Corona_Vaccination_Population v1
                        --on v1.Person_Id = p.new_IdentityNumber
                        --and v1.dose_ind = 2 

                        left join full_sequence_res f1
                        on f1.id = p.patient_id 
                        and f1.variant_number = 1

                        left join last_take_dates
                        on last_take_dates.id = ids.id

                        left join dwh.Fact_Incoming_Flights_Forms f2
                        on f2.id_number = p.new_IdentityNumber 
                        and f2.arrival_date > getdate() - 180 
                        and f2.visited_country not in ('ישראל', 'Israel')
                        and f2.id_number is not null
                        and f2.id_number != ''
                        and f2.arrival_date is not null

						left join dwh.Fact_Corona_Vaccination_Population v
						on v.Person_Id = p.new_IdentityNumber
						
						left join dwh.Fact_Corona_serology_Test_Model ser
						on p.patient_ID = ser.patient_ID and datediff(day, ser.new_takedate, getdate()) between 0 and 183 and ser.new_coronaresulttestname = 'חיובי סרולוגיה'

                        """

        data_from_bi = bi_conn.query_by_str(bi_query)

        data_from_bi['variant'] = data_from_bi['variant'].astype(str)
        data_from_bi['variant'] = data_from_bi['variant'].apply(map_to_variant_name)

        id_numbers = list(data_from_bi['id_number'])
        my_select = build_select(id_numbers)

        xrm_query = f"""
        select distinct
            i.new_contactidentification as 'id_number'
            , i.new_workplace as 'work'
            , a.new_name as 'work XRM'
        from ({my_select}) ids
        join dbo.new_NCoronaVInvestigationBase i
        on i.new_contactidentification = ids.id
        join new_instituteBase a
        on a.new_instituteId = i.new_WorkPlaceId
        """

        data_from_xrm = xrm_conn.query_by_str(xrm_query)

        info_df = pd.merge(data_from_bi, data_from_xrm, how='left', on='id_number')

        info_df['was_ever_positive'] = info_df['was_ever_positive'].replace({1: True, 0: False})

        description_columns = ['full_name', 'age', 'city', 'final_id', 'was_ever_positive']

        color_mapper = {
            True: '#FF0000',
            False: '#FFFF00'
        }

        variant_color_dict = {
            'uganda': '#5C4000', # brown
            'chile': '#FFAA00', #orange
            'india': '#00FF00', #green
            'uk': '#00FFFF', #blue-white
            '484': '#8600CF', #purple
            'new york': '#FF00C8', #pink
            'israel': '#0000FF', #blue
            'sa': '#000000', #black
            'global': '#9999FF', #wierd blue
            'st. petersburg': '#00FFA6', #green discus
            'california': '#FFFFFF', #white
            'manaus': '#095700', #green black
            'omicron': '#CC0066', #bright magneta
            # 'variant found of no group': '#C4C4C4' #grey
        }

        for _, row in info_df.iterrows():
            patient_id = row['patient_ID']
            data = row.drop(index='patient_ID')

            description = row[description_columns].copy()
            description = description.fillna('?')
            description = '-'.join([str(s) for s in description])
            data.loc['summary'] = description
            data.loc['color'] = color_mapper[data.loc['was_ever_positive']]

            try:
                if(data.loc['variant'] in list(variant_color_dict.keys())):
                    data.loc['color'] = variant_color_dict[data.loc['variant']]
            except:
                print('error giving variant color', data)


            if anonymous_name:
                data.loc['full_name'] = data.loc['full_name'][0] + '-' + data.loc['id_number'][:3]

            res_dict[patient_id] = data

        for p in patient_ids:
            if p not in res_dict:
                res_dict[p] = pd.Series(index=data_from_bi.columns.drop('patient_ID'))

    for p in res_dict:
        if res_dict[p].empty:
            print('Attention - no information was found for patient', p)

    return res_dict