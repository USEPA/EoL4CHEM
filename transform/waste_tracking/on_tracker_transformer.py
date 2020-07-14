# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Importing libraries
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import os
import re
from functools import reduce
import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)
import argparse

class On_Tracker:

    def __init__(self, year = None):
        self.year = year
        self._dir_path = os.path.dirname(os.path.realpath(__file__)) # Working Directory
        #self._dir_path = os.getcwd() # if you are working on Jupyter Notebook

    # Function for calculating weighted average and avoiding ZeroDivisionError, which ocurres
    # "when all weights along axis are zero".
    def _weight_mean(self, v, w):
        try:
            return round(np.average(v, weights = w))
        except ZeroDivisionError:
            try:
                return round(v.mean())
            except ValueError:
                return np.nan


    def _organizing_flows(self, classification, flow, bs):
        if classification == 'TRI':
            return tuple((0.5 if bs[i] and float(v) == 0.0 else float(v)) if re.search(r'[^A-Za-z]\s?[0-9]+\.?[0-9]*\s?', v) else None for i, v in enumerate(flow))
        elif classification == 'DIOXIN':
            return tuple((0.00005 if bs[i] and float(v) == 0.0 else float(v)) if re.search(r'[^A-Za-z]\s?[0-9]+\.?[0-9]*\s?', v) else None for i, v in enumerate(flow))
        elif classification == 'PBT':
            return tuple((0.1 if bs[i] and float(v) == 0.0 else float(v)) if re.search(r'[^A-Za-z]\s?[0-9]+\.?[0-9]*\s?', v) else None for i, v in enumerate(flow))
        else:
            return tuple((0.0 if bs[i] and float(v) == 0.0 else float(v)) if re.search(r'[^A-Za-z]\s?[0-9]+\.?[0-9]*\s?', v) else None for i, v in enumerate(flow))


    def Organizing_releases(self):
        mapping = {'M': 1, 'M1': 1, 'M2': 1, 'E': 2,
                'E1': 2, 'E2': 2, 'C': 3, 'O': 4,
                'X': 5, 'N': 5, 'NA': 5}
        def organizing_NAICS(x):
            try:
                result = re.search(r'([0-9]+).?0*', x)
                return result.group(1)
            except TypeError:
                return None
        columns_converting = {'CAS NUMBER': lambda x: x.lstrip('0')}
        if int(str(self.year)) >= 2011:
            Files = ['1a', '3a', '3c']
        else:
            Files = ['1a', '3a', '3b']
        dfs = list()
        columns_flow_T = list()
        columns_flow_T_DQ = list()
        for File in Files:
            Path_csv = self._dir_path + '/../../Extract/TRI/CSV/US_' + File + '_' + self.year + '.csv'
            Path_txt = self._dir_path + '/../../Ancillary/TRI/TRI_File_' + File + '_needed_columns_tracking.txt'
            df_columns = pd.read_csv(Path_txt, header = None, sep = ',')
            columns_needed =  df_columns.iloc[:, 0].tolist()
            df = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False,
                            converters = columns_converting, usecols = columns_needed,
                            dtype = str)
            columns_flow = [col.replace(' - BASIS OF ESTIMATE', '') for col in columns_needed if ' - BASIS OF ESTIMATE' in col]
            if File == '1a':
                df['PRIMARY NAICS CODE'] =  df['PRIMARY NAICS CODE'].apply(lambda x: organizing_NAICS(x))
                df['MAXIMUM AMOUNT ON-SITE'] = pd.to_numeric(df['MAXIMUM AMOUNT ON-SITE'], errors = 'coerce')
                columns_releases = df_columns.loc[df_columns.iloc[:, 1] == 'Release', 0].tolist()
                df_compartments = df_columns.loc[pd.notnull(df_columns.iloc[:, 2]), [0, 2]]
                on_site_managment_different_disposal = ['ON-SITE - ENERGY RECOVERY', 'ON-SITE - RECYCLED', 'ON-SITE - TREATED']
                on_site_managment_different_disposal_DQ = [col + ' - BASIS OF ESTIMATE' for col in on_site_managment_different_disposal]
                df = df.assign(**{col + ' - BASIS OF ESTIMATE': 'M' for col in on_site_managment_different_disposal})
                columns_flow = columns_flow + on_site_managment_different_disposal
                del on_site_managment_different_disposal
            if File == '3a':
                df.drop(columns = ['OFF-SITE RCRA ID NR', 'OFF-SITE COUNTRY ID', 'REPORTING YEAR', 'UNIT OF MEASURE'], inplace = True)
            df[columns_flow] = df[columns_flow].where(pd.notnull(df[columns_flow]), '0.0')
            columns_DQ = [col + ' - BASIS OF ESTIMATE' for col in columns_flow]
            df[columns_DQ] = df[columns_DQ].where(pd.notnull(df[columns_DQ]), None)
            if File == '1a':
                columns_releases_DQ  = [col + ' - BASIS OF ESTIMATE' for col in columns_releases]
                df[columns_releases] = df.apply(lambda x: pd.Series(self._organizing_flows(x['CLASSIFICATION'], \
                                                 x[columns_releases].tolist(), x[columns_releases_DQ].tolist())),
                                                 axis = 1)
                df.drop(columns = ['CLASSIFICATION'], inplace = True)
            df[columns_DQ] = df.apply(lambda row: pd.Series('M' if (row[columns_flow[i]] == 0.0 and pd.isnull(row[columns_DQ[i]])) \
                                  else ('X' if (row[columns_flow[i]] != 0.0 and pd.isnull(row[columns_DQ[i]])) \
                                  else row[columns_DQ[i]]) for i in range(len(columns_flow))), axis = 1)
            df[columns_flow] = df[columns_flow].apply(pd.to_numeric, errors = 'coerce')
            df = df.loc[pd.notnull(df).all(axis =  1)]
            df[columns_DQ] = df[columns_DQ].apply(lambda x: x.str.strip().map(mapping), axis = 1)
            df[columns_DQ] = df[columns_DQ].fillna(value = 5)
            # Grouping rows
            func = {column:'sum' for column in columns_flow}
            func.update({columns_DQ[i]: lambda x: self._weight_mean(x, df.loc[x.index, col]) for i, col in enumerate(columns_flow)})
            grouping = list(set(df.columns.tolist()) - set(columns_flow + columns_DQ))
            df = df.groupby(grouping, as_index = False).agg(func)
            columns_flow_T = columns_flow_T + columns_flow
            columns_flow_T_DQ = columns_flow_T_DQ + columns_DQ
            dfs.append(df)
            del df, columns_flow, columns_DQ, grouping, func
        # Joining databases
        df = reduce(lambda  left, right: pd.merge(left , right,
                        on = ['TRIFID', 'CAS NUMBER'],
                        how = 'left'), dfs)
        del dfs
        df[columns_flow_T] = df[columns_flow_T].fillna(value = 0.0)
        df[columns_flow_T_DQ] = df[columns_flow_T_DQ].fillna(value = 1)
        df.loc[df['UNIT OF MEASURE'] == 'Pounds', columns_flow_T] *= 0.453592
        df.loc[df['UNIT OF MEASURE'] == 'Grams', columns_flow_T] *= 10**-3
        df[columns_flow_T] = df[columns_flow_T].round(6)
        df['UNIT OF MEASURE'] = 'kg'
        df['TOTAL WASTE'] = df[columns_flow_T].sum(axis = 1)
        df['TOTAL WASTE RELIABILITY'] = df.apply(lambda row: self._weight_mean(row[columns_flow_T_DQ], row[columns_flow_T]), axis = 1)
        columns_releases_DQ = [col + ' - BASIS OF ESTIMATE' for col in columns_releases]
        df['TOTAL RELEASE'] = df[columns_releases].sum(axis = 1)
        df['TOTAL RELEASE RELIABILITY'] = df.apply(lambda row: self._weight_mean(row[columns_releases_DQ], row[columns_releases]), axis = 1)
        compartments = ['Fugitive air emissions', 'Stack air emissions', 'On-site surface water', 'On-site soil']
        cols_to_conserve = ['CAS NUMBER', 'PRIMARY NAICS CODE', 'REPORTING YEAR',
                            'UNIT OF MEASURE', 'MAXIMUM AMOUNT ON-SITE', 'TRIFID',
                            'TOTAL WASTE', 'TOTAL WASTE RELIABILITY', 'TOTAL RELEASE',
                            'TOTAL RELEASE RELIABILITY']
        df_release = pd.DataFrame()
        for compartment in compartments:
            df_aux = df[cols_to_conserve]
            df_aux['COMPARTMENT'] = compartment
            compartment_flow = df_compartments.loc[df_compartments[2] == compartment, 0].tolist()
            compartment_DQ = [col + ' - BASIS OF ESTIMATE' for col in compartment_flow]
            df_aux['FLOW TO COMPARTMENT'] = df[compartment_flow].sum(axis = 1)
            df_aux['FLOW TO COMPARTMENT RELIABILITY'] = df.apply(lambda row: self._weight_mean(row[compartment_DQ], row[compartment_flow]), axis = 1)
            df_release = pd.concat([df_release, df_aux], axis = 0,
                                     ignore_index = True,
                                     sort = True)
        del df, df_aux, compartments, cols_to_conserve
        # Calling NAICS file
        NAICS = pd.read_csv(self._dir_path + '/../../Ancillary/Others/NAICS_Structure.csv',
                              header = 0,
                              sep = ',',
                              converters = {'NAICS Title':lambda x: x.capitalize()},
                              dtype = {'NAICS Code': 'object'})
        NAICS.rename(inplace = True, columns = {'NAICS Code':'PRIMARY NAICS CODE'})
        df_release =  pd.merge(df_release, NAICS, how =  'left', on = 'PRIMARY NAICS CODE')
        columns = ['REPORTING YEAR', 'TRIFID' , 'PRIMARY NAICS CODE', 'NAICS Title',
                 'CAS NUMBER', 'MAXIMUM AMOUNT ON-SITE', 'UNIT OF MEASURE',
                 'FLOW TO COMPARTMENT', 'FLOW TO COMPARTMENT RELIABILITY', 'COMPARTMENT',
                 'TOTAL WASTE', 'TOTAL WASTE RELIABILITY', 'TOTAL RELEASE', 'TOTAL RELEASE RELIABILITY']
        df_release = df_release[columns]
        print(df_release.loc[pd.isnull(df_release['NAICS Title']), 'PRIMARY NAICS CODE'].unique())
        df_release.to_csv(self._dir_path + '/CSV/On_site_tracking/TRI_releases_' + self.year + '.csv',
                           sep = ',', index = False)


    def Facility_information(self):
        df_TRI = pd.DataFrame()
        Files = [file for file in os.listdir(self._dir_path + '/../../Extract/TRI/CSV') if 'US_1a' in file]
        for File in Files:
            Path_csv = self._dir_path + '/../../Extract/TRI/CSV/{}'.format(File)
            TRI_aux = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False,
                                 usecols = ['TRIFID', 'FACILITY NAME', 'FACILITY STREET',
                                           'FACILITY CITY',	'FACILITY COUNTY', 'FACILITY STATE',
                                           'FACILITY ZIP CODE'])
            df_TRI = pd.concat([TRI_aux, df_TRI], ignore_index = True, axis = 0)
        df_TRI.drop_duplicates(keep = 'first', inplace = True, subset = ['TRIFID'])
        df_TRI.to_csv(self._dir_path + '/CSV/On_site_tracking/Facility_Information.csv',
                    sep = ',', index = False)

    def Conditions_of_use(self):
        df_TRI = pd.DataFrame()
        Files = [file for file in os.listdir(self._dir_path + '/../../Extract/TRI/CSV') if 'US_1b' in file]
        for File in Files:
            Path_csv = self._dir_path + '/../../Extract/TRI/CSV/{}'.format(File)
            TRI_aux = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False,
                             usecols = ['REPORTING YEAR', 'TRIFID', 'CAS NUMBER',
                                        'PRODUCE THE CHEMICAL', 'IMPORT THE CHEMICAL',
                                        'ON-SITE USE OF THE CHEMICAL', 'SALE OR DISTRIBUTION OF THE CHEMICAL',
                                        'AS A BYPRODUCT', 'AS A MANUFACTURED IMPURITY',
                                        'USED AS A REACTANT', 'ADDED AS A FORMULATION COMPONENT',
                                        'USED AS AN ARTICLE COMPONENT', 'REPACKAGING',
                                        'AS A PROCESS IMPURITY', 'RECYCLING',
                                        'USED AS A CHEMICAL PROCESSING AID',
                                        'USED AS A MANUFACTURING AID', 'ANCILLARY OR OTHER USE'])
            df_TRI = pd.concat([TRI_aux, df_TRI], ignore_index = True, axis = 0)
        df_TRI.drop_duplicates(keep = 'first', inplace = True, subset = ['TRIFID'])
        df_TRI.to_csv(self._dir_path + '/CSV/On_site_tracking/Conditions_of_use_by_facility_and_chemical.csv',
                    sep = ',', index = False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A]: Organize maximum and releases\
                        [B]: Facility information\
                        [C]: Condition of use', \
                        type = str)

    parser.add_argument('-Y', '--Year',
                        nargs = '+',
                        help = 'What TRI year do you want to organize?.',
                        type = str,
                        required = False,
                        default = None)

    args = parser.parse_args()

    TRIyears = args.Year

    if args.Option == 'A':
        for TRIyear in TRIyears:
            T = On_Tracker(TRIyear)
            T.Organizing_releases()
    if args.Option == 'B':
        T = On_Tracker()
        T.Facility_information()
    if args.Option == 'C':
        T = On_Tracker()
        T.Conditions_of_use()
