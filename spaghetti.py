# This file contains functions that mainly focus on creating nodes and edges.
# It also has a function that enables the possibility of running spaghetti on multiple templates
# And, handles the dataframe connection with the system.

import datetime
import os

from Utils.enrichments import hashfunc
from Utils.queries import get_patient_ids, perform_exploration,\
    get_patient_information
from Utils.SQLConnector import SQLConnector
from Utils.mail_sender import send_mail
import networkx as nx
import pandas as pd
import numpy as np


class Spaghetti(nx.DiGraph):
    # This function initializes the spaghetti object.
    def __init__(self):
        super().__init__()
        self.explored = {}
        self.edge_to_date = {}
        self.bi_conn = SQLConnector(db='bi')
        self.xrm_conn = SQLConnector(db='xrm')
        self.origin_nodes = None

    # This function creates nodes using the patient id received from an id/epi/phone number.
    def build_graph(self, epi_numbers, id_numbers, phone_numbers):
        self.__init__()
        epi_numbers = [int(e) for e in epi_numbers if e is not None]
        id_numbers = [str(i).replace("'", '') for i in id_numbers if i is not None]
        modified_id_numbers = []
        for id_number in id_numbers:
            if len(id_number) < 9 and id_number.isdigit():
                new_id_number = '0'*(9 - len(id_number)) + id_number
                modified_id_numbers.append(new_id_number)

        id_numbers = id_numbers + modified_id_numbers
        modified_id_numbers = []
        for id_number in id_numbers:
            for i in range(len(id_number)):
                if id_number[i] == '0' and id_number.isdigit():
                    modified_id_numbers.append(id_number[i:])
                else:
                    break

        id_numbers = id_numbers + modified_id_numbers
        id_numbers = list(set(id_numbers))

        if len(epi_numbers) == 0 and len(id_numbers) == 0:
            print('no valid input was inserted.')
            return

        patients = get_patient_ids(self.bi_conn, id_numbers, epi_numbers, phone_numbers)

        for patient_id in patients:
            self.add_node(patient_id)

        self.origin_nodes = list(self.nodes)

    # This function finds exposures and connects them depending on the date of exposure and connection.
    def explore(self, direction='up', levels=2, days_back=14):
        if direction not in self.explored:
            self.explored[direction] = set()

        print('Starting exploring', direction)
        for i in range(levels):
            unexplored_ids = [p for p in self.nodes if p not in self.explored[direction]]

            print('Exploring at level', i+1, 'for', len(unexplored_ids), 'patients at', datetime.datetime.now())

            if len(unexplored_ids) == 0:
                break

            results = perform_exploration(unexplored_ids, self.bi_conn, direction, days_back)
            for father_patient_id, son_patient_id, contact_date in results:

                self.add_edge(father_patient_id, son_patient_id)

                edge = (father_patient_id, son_patient_id)
                if edge not in self.edge_to_date:
                    self.edge_to_date[edge] = contact_date
                else:
                    self.edge_to_date[edge] = min(contact_date, self.edge_to_date[edge])

            for patient in unexplored_ids:
                self.explored[direction].add(patient)

        print('Finished exploring', direction)

    # This function defines the edges a father and a son.
    def fix_edges(self, info_mapper):

        new_edge_to_date = {}

        for father, son in self.edges:

            contact_date = self.edge_to_date[(father, son)]

            if (son, father) in self.edges:
                other_contact_date = self.edge_to_date[(son, father)]
                contact_date = np.min([contact_date, other_contact_date])

            father_epi_number = info_mapper[father]['epi_number']
            son_epi_number = info_mapper[son]['epi_number']

            if pd.isna(son_epi_number):
                real_father = father
                real_son = son

            elif pd.isna(father_epi_number):
                real_father = son
                real_son = father

            elif father_epi_number < son_epi_number:
                real_father = father
                real_son = son

            else:
                real_father = son
                real_son = father

            if info_mapper[real_father].loc['was_ever_positive']:
                new_edge_to_date[(real_father, real_son)] = contact_date

        self.clear_edges()
        self.add_edges_from(list(new_edge_to_date.keys()))
        self.edge_to_date = new_edge_to_date

    # This function adds the chain-id to the excel, also adds the related epi numbers to the excel
    # it also connects the neighbors to the patient depending on the fix_edges function
    def summarize(self):

        print(f'Summarizing results at {datetime.datetime.now()}')

        print(f'Getting patient information at {datetime.datetime.now()}')
        info_mapper = get_patient_information(list(self.nodes), self.bi_conn, self.xrm_conn)

        self.fix_edges(info_mapper)

        pos_nodes = {node for node in self.nodes if info_mapper[node].loc['was_ever_positive']}
        neg_nodes = set(self.nodes) - pos_nodes

        print(f'Stats: {len(pos_nodes)} positives, {len(neg_nodes)} negatives and {len(self.edges)} edges.')

        connected_components = nx.connected_components(self.to_undirected())
        patient_to_component = {}
        patient_to_component_epi_numbers = {}

        for component in connected_components:
            component_epi_numbers = [int(info_mapper[c]['epi_number']) for c in component
                                     if pd.notna(info_mapper[c]['epi_number'])]

            for patient in component:
                patient_to_component[patient] = list(component)
                patient_to_component_epi_numbers[patient] = component_epi_numbers

        print(f'Writing results at {datetime.datetime.now()}')

        all_rows = []

        for father in self.nodes:

            father_info = info_mapper[father].copy()

            if not (father_info.loc['was_ever_positive'] or father in self.origin_nodes):
                continue

            father_info.index = ['patient_' + i for i in father_info.index]

            neighbors = list(self.neighbors(father))

            if father in self.origin_nodes and len(neighbors) == 0:
                neighbors.append(father)

            for son in neighbors:

                edge = (father, son)

                son_info = info_mapper[son].copy()
                son_info.index = ['contact_' + i for i in son_info.index]

                row = father_info.append(son_info)

                row.loc['date_of_contact'] = pd.NaT
                if edge in self.edge_to_date:
                    row.loc['date_of_contact'] = self.edge_to_date[edge]

                row.loc['related_epi_numbers'] = patient_to_component_epi_numbers[son]
                row.loc['chain_id'] = hashfunc(row.loc['related_epi_numbers'])

                all_rows.append(row.copy())

        res_df = pd.concat(all_rows, axis=1).transpose()
        res_df = res_df.sort_values(by=['date_of_contact', 'patient_epi_number'])
        res_df = res_df.reset_index(drop=True)

        return res_df

    # This function runs the spaghetti from a dataframe, gets a dataframe and returns the dataframe with the spaghetti information.
    @staticmethod
    def run_on_dataframe(df: pd.DataFrame,
                         fname = '',
                         directory = '',
                         epi_number_col='מספר אפידימיולוגי',
                         id_number_col='תעודת זהות',
                         phone_number_col='מספר טלפון',
                         levels_up=2,
                         levels_down=6,
                         days_back=14,
                         save=True,
                         summarize=True,
                         to_send_email=True,
                         sender='gilad.goldreich@moh.gov.il',
                         receiver='gilad.goldreich@moh.gov.il'):

        if df.empty:
            print('No data was inserted')
            return None

        if epi_number_col not in df.columns and id_number_col not in df.columns:
            error_message = f"Error - {fname} received at {datetime.datetime.now()} is an invalid file - " \
                            f"both {epi_number_col} and {id_number_col} do not exist"
            print(error_message)
            if to_send_email:
                try:
                    subject = error_message
                    send_mail(sender, receiver, subject)
                except Exception as e:
                    print('Unable to send email - received the following Exception:')
                    print(e)
            return None

        epi_numbers = []
        id_numbers = []
        phone_numbers = []

        if epi_number_col in df.columns:
            epi_numbers = df[epi_number_col].dropna()
        if id_number_col in df.columns:
            id_numbers = df[id_number_col].dropna()
            id_numbers = [int(x) if isinstance(x, float) else x for x in id_numbers]
        if phone_number_col in df.columns:
            phone_numbers = df[phone_number_col].dropna()

        if len(epi_numbers) == 0 and len(id_numbers) == 0:
            print('No data was inserted')
            return None

        g = Spaghetti()
        g.build_graph(epi_numbers, id_numbers, phone_numbers)

        if not len(g.nodes):
            print('Given patients do not exist in the system')
            return None

        g.explore('up', levels_up, days_back)
        g.explore('down', levels_down, days_back)

        if summarize:
            res_df = g.summarize()

            if save:
                out_fname = f"results_for_{fname}_at_{datetime.datetime.now().strftime('%Y-%m-%d-%H.%M')}"
                out_fname = out_fname.replace(".xlsx", "")
                out_fname = f"{out_fname}.xlsx"

                out_dir = os.path.join(directory, 'out')
                if not os.path.isdir(out_dir):
                    os.mkdir(out_dir)

                out_path = os.path.join(out_dir, out_fname)
                print('Saving results at', out_path)
                res_df.to_excel(out_path, index=False)

                if to_send_email:
                    print('Sending email to', receiver, 'from', sender)

                    try:
                        subject = f"Results for {fname} at {datetime.datetime.now()} using {levels_up} levels up, " \
                                  f"{levels_down} levels down and {days_back} days back"
                        send_mail(sender, receiver, subject, out_path)

                    except Exception as e:
                        print('Unable to send email - received the following Exception:')
                        print(e)

            return res_df

        else:
            return g

    # This function allows you to run the dataframe function on multiple templates.
    @staticmethod
    def run_on_directory(directory,
                         epi_number_col='מספר אפידימיולוגי',
                         id_number_col='תעודת זהות',
                         levels_up=2,
                         levels_down=6,
                         days_back=14,
                         to_send_email=True,
                         save=True,
                         summarize = True,
                         sender='gilad.goldreich@moh.gov.il',
                         receiver='gilad.goldreich@moh.gov.il'):

        results = []

        if not os.path.isdir(directory):
            print('directory', directory, 'does not exist')
            return results

        in_dir = os.path.join(directory, 'in')

        if not os.path.isdir(in_dir):
            print('directory', in_dir, 'does not exist')
            return results

        files_names = [f for f in os.listdir(in_dir) if f.endswith('.xlsx')]

        for fname in files_names:

            in_path = os.path.join(in_dir, fname)
            df = pd.read_excel(in_path)
            print('running on', in_path)
            res_df = Spaghetti.run_on_dataframe(df=df,
                                                fname=fname,
                                                directory=directory,
                                                epi_number_col=epi_number_col,
                                                id_number_col=id_number_col,
                                                levels_up=levels_up,
                                                levels_down=levels_down,
                                                days_back=days_back,
                                                save=save,
                                                summarize=summarize,
                                                to_send_email=to_send_email,
                                                sender=sender,
                                                receiver=receiver)

            if res_df is None or res_df.empty:
                continue

            results.append(res_df.copy())

        return results


if __name__ == '__main__':
    Spaghetti.run_on_directory(directory=r'\\fsmtt\public\Idf-Data\פיצוח\Pakar\spaghetti',
                               epi_number_col='מספר אפידימיולוגי',
                               id_number_col='תעודת זהות',
                               levels_up=3,
                               levels_down=10,
                               days_back=60,
                               to_send_email=False,
                               sender='gilad.goldreich@moh.gov.il',
                               receiver='5079371@moh.gov.il'
                               )
