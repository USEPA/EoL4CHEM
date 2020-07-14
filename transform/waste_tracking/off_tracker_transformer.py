# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Importing libraries
import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None
import os
import re
import warnings
warnings.simplefilter(action = 'ignore', category = FutureWarning)
import argparse

class Off_Tracker:

    def __init__(self, year, database):
        self.year = year
        self.database = database
        self._dir_path = os.path.dirname(os.path.realpath(__file__)) # Working Directory
        #self._dir_path = os.getcwd() # if you are working on Jupyter Notebook

    # Function for calculating weighted average and avoiding ZeroDivisionError, which ocurres
    # "when all weights along axis are zero".
    def _weight_mean(self, v, w):
        try:
            return round(np.average(v, weights = w))
        except ZeroDivisionError:
            return round(v.mean())


    def retrieving_needed_information(self):
        columns_converting = {'REPORTING YEAR': lambda x: str(int(x)), \
                            'CAS NUMBER': lambda x: x.lstrip('0')}
        if self.database == 'TRI':
            mapping = {'M': 1, 'M1': 1, 'M2': 1, 'E': 2,
                    'E1': 2, 'E2': 2, 'C': 3, 'O': 4,
                    'X': 5, 'N': 5, 'NA': 5}
            cols = ['REPORTING YEAR', 'SENDER FRS ID', 'SENDER LATITUDE', 'SENDER LONGITUDE', \
                    'SRS INTERNAL TRACKING NUMBER', 'CAS', 'QUANTITY TRANSFERRED', 'RELIABILITY', \
                    'FOR WHAT IS TRANSFERRED', 'UNIT OF MEASURE', 'RECEIVER FRS ID', 'RECEIVER TRIFID', \
                    'RECEIVER LATITUDE', 'RECEIVER LONGITUDE']
            Path_txt = self._dir_path + '/../../Ancillary/TRI/TRI_File_3a_needed_columns_tracking.txt'
            columns_needed = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:,0].tolist()
            Path_csv = self._dir_path + '/../../Extract/TRI/CSV/US_3a_' + self.year + '.csv'
            df = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False,
                            converters = columns_converting, usecols = columns_needed)
            df = df.loc[~pd.notnull(df['OFF-SITE COUNTRY ID'])]
            df.drop(columns = ['OFF-SITE COUNTRY ID'], inplace = True)
            column_flows = [col.replace(' - BASIS OF ESTIMATE', '') for col in columns_needed if 'BASIS OF ESTIMATE' in col]
            df[column_flows].fillna(value = 0, inplace = True)
            df = df.loc[df['OFF-SITE RCRA ID NR'].str.contains('\s?[A-Z]{2,3}[0-9]{8,9}\s?',  na = False)]
            df = df.loc[(df[column_flows] != 0).any(axis = 1)]
            Names = [re.sub(r'potws', 'POTWS', re.sub(r'Rcra|rcra', 'RCRA', col.replace('OFF-SITE - ', '').strip().capitalize())) for col in column_flows]
            columns =  ['TRIFID', 'CAS NUMBER', 'UNIT OF MEASURE', \
                        'REPORTING YEAR', 'OFF-SITE RCRA ID NR', \
                        'QUANTITY TRANSFERRED', 'RELIABILITY', \
                        'FOR WHAT IS TRANSFERRED']
            _df = pd.DataFrame(columns = columns)
            for col in column_flows:
                df_aux = df[['TRIFID', 'CAS NUMBER', 'UNIT OF MEASURE', \
                            'REPORTING YEAR', 'OFF-SITE RCRA ID NR', \
                            col, col + ' - BASIS OF ESTIMATE']]
                df_aux.rename(columns = {col: 'QUANTITY TRANSFERRED',
                                        col + ' - BASIS OF ESTIMATE': 'RELIABILITY'},
                            inplace = True)
                df_aux['FOR WHAT IS TRANSFERRED'] = re.sub(r'potws', 'POTWS', re.sub(r'Rcra|rcra', 'RCRA', col.replace('OFF-SITE - ', '').strip().capitalize()))
                _df = pd.concat([_df, df_aux], ignore_index = True,
                                      sort = True, axis = 0)
            del df, df_aux
            _df =  _df.loc[_df['QUANTITY TRANSFERRED'] != 0.0]
            _df.loc[_df['UNIT OF MEASURE'] == 'Pounds', 'QUANTITY TRANSFERRED'] *= 0.453592
            _df.loc[_df['UNIT OF MEASURE'] == 'Grams', 'QUANTITY TRANSFERRED'] *= 10**-3
            _df['UNIT OF MEASURE'] = 'kg'
            _df['RELIABILITY'] = _df['RELIABILITY'].str.strip().map(mapping)
            _df['RELIABILITY'].fillna(value = 5, inplace = True)
            func = {'QUANTITY TRANSFERRED': 'sum',
                    'RELIABILITY': lambda x: self._weight_mean(x, _df.loc[x.index, 'QUANTITY TRANSFERRED'])}
            _df = _df.groupby(['TRIFID', 'CAS NUMBER', 'UNIT OF MEASURE', \
                            'REPORTING YEAR', 'OFF-SITE RCRA ID NR', \
                            'FOR WHAT IS TRANSFERRED'],
                             as_index = False).agg(func)
            # Searching EPA Internal Tracking Number of a Substance
            SRS = self._Generating_SRS_Database()
            _df = pd.merge(SRS, _df, how = 'inner', left_on = 'ID', right_on = 'CAS NUMBER')
            _df['SRS INTERNAL TRACKING NUMBER'] = _df['Internal Tracking Number']
            _df.drop(['ID', 'CAS NUMBER', 'Internal Tracking Number'], axis = 1, inplace = True)
            # Searching info for sender
            FRS = self._Generating_FRS_Database(['TRIS', 'RCRAINFO'])
            RCRA = FRS.loc[FRS['PGM_SYS_ACRNM'] == 'RCRAINFO']
            TRI = FRS.loc[FRS['PGM_SYS_ACRNM'] == 'TRIS']
            del FRS
            RCRA.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
            TRI.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
            _df = pd.merge(_df, TRI, how = 'inner', left_on = 'TRIFID', right_on = 'PGM_SYS_ID')
            _df['SENDER FRS ID'] = _df['REGISTRY_ID']
            _df['SENDER LATITUDE'] = _df['LATITUDE83']
            _df['SENDER LONGITUDE'] = _df['LONGITUDE83']
            _df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'TRIFID', 'LATITUDE83', 'LONGITUDE83'], \
                    axis = 1, inplace = True)
            # Searching info for receiver
            _df = pd.merge(_df, RCRA, how = 'inner', left_on = 'OFF-SITE RCRA ID NR', right_on = 'PGM_SYS_ID')
            _df['RECEIVER FRS ID'] = _df['REGISTRY_ID']
            _df['RECEIVER LATITUDE'] = _df['LATITUDE83']
            _df['RECEIVER LONGITUDE'] = _df['LONGITUDE83']
            _df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'OFF-SITE RCRA ID NR', 'LATITUDE83', 'LONGITUDE83'], \
                     axis = 1, inplace = True)
            _df = pd.merge(_df, TRI, how = 'left', left_on = 'RECEIVER FRS ID', right_on = 'REGISTRY_ID')
            _df['RECEIVER TRIFID'] = _df['PGM_SYS_ID']
            _df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'LATITUDE83', 'LONGITUDE83'], \
                     axis = 1, inplace = True)
            _df = _df[cols]
            _df.drop_duplicates(keep = 'first', inplace = True)
            _df.to_csv(self._dir_path + '/CSV/Off_site_tracking/TRI_' + self.year + '_Off-site_Tracking.csv',
                            sep = ',',  index = False)
        else:
            cols = ['REPORTING YEAR', 'SENDER FRS ID', 'SENDER LATITUDE', 'SENDER LONGITUDE', \
                    'SRS INTERNAL TRACKING NUMBER', 'CAS', 'WASTE SOURCE CODE', 'QUANTITY RECEIVED', \
                    'QUANTITY TRANSFERRED', 'RELIABILITY', 'FOR WHAT IS TRANSFERRED', 'UNIT OF MEASURE', \
                    'RECEIVER FRS ID', 'RECEIVER TRIFID', 'RECEIVER LATITUDE', 'RECEIVER LONGITUDE']
            Path_txt = self._dir_path + '/../../Ancillary/RCRAInfo/RCRAInfo_needed_columns.txt'
            columns_needed = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:,0].tolist()
            Path_csv = self._dir_path + '/../../Extract/RCRAInfo/CSV/BR_REPORTING_' + self.year +'.csv'
            df = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False, usecols = columns_needed)
            #df = df.loc[pd.notnull(df['Total Quantity Shipped Off-site (in tons)'])  \
            #            & df['Total Quantity Shipped Off-site (in tons)'] != 0.0]
            df['QUANTITY TRANSFERRED'] = df['Total Quantity Shipped Off-site (in tons)']*907.18
            df['QUANTITY RECEIVED'] = df['Quantity Received (in tons)']*907.18
            df['UNIT OF MEASURE'] = 'kg'
            df = df.loc[(df['QUANTITY TRANSFERRED'] != 0) | (df['QUANTITY RECEIVED'] != 0)]
            # Searching EPA Internal Tracking Number of a Substance
            SRS = self._Generating_SRS_Database(['RCRA_T', 'RCRA_F', 'RCRA_K', 'RCRA_P', 'RCRA_U'])
            df = pd.merge(SRS, df, how = 'inner', left_on = 'ID', right_on = 'Waste Code Group')
            df['SRS INTERNAL TRACKING NUMBER'] = df['Internal Tracking Number']
            df.drop(['ID', 'Waste Code Group', 'Internal Tracking Number', \
                    'Total Quantity Shipped Off-site (in tons)',
                    'Quantity Received (in tons)'], axis = 1, inplace = True)
            Received = df.loc[df['QUANTITY RECEIVED'] != 0]
            Received.drop(columns = ['Waste Source Code', 'QUANTITY TRANSFERRED'], inplace = True)
            group = ['CAS', 'EPA Handler ID', 'Reporting Cycle Year',
                    'Management Method Code',  'EPA ID Number of Facility to Which Waste was Shipped',
                    'UNIT OF MEASURE', 'SRS INTERNAL TRACKING NUMBER']
            Received = Received.groupby(group, as_index = False)\
                                .agg({'QUANTITY RECEIVED': lambda x: x.sum()})
            Transferred = df.loc[df['QUANTITY TRANSFERRED'] != 0]
            Transferred.drop(columns = ['QUANTITY RECEIVED'], inplace = True)
            group = ['CAS', 'EPA Handler ID', 'Reporting Cycle Year', 'Waste Source Code',
                    'Management Method Code',  'EPA ID Number of Facility to Which Waste was Shipped',
                    'UNIT OF MEASURE', 'SRS INTERNAL TRACKING NUMBER']
            Transferred = Transferred.groupby(group, as_index = False)\
                                .agg({'QUANTITY TRANSFERRED': lambda x: x.sum()})
            Transferred['RELIABILITY'] = 1
            df = pd.concat([Received, Transferred], ignore_index = True,
                                sort = True, axis = 0)
            del Received, Transferred
            # Searching info for sender
            FRS = self._Generating_FRS_Database(['TRIS', 'RCRAINFO'])
            RCRA = FRS.loc[FRS['PGM_SYS_ACRNM'] == 'RCRAINFO']
            TRI = FRS.loc[FRS['PGM_SYS_ACRNM'] == 'TRIS']
            del FRS
            RCRA.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
            TRI.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
            df = pd.merge(df, RCRA, how = 'inner', left_on = 'EPA Handler ID', right_on = 'PGM_SYS_ID')
            df['SENDER FRS ID'] = df['REGISTRY_ID']
            df['SENDER LATITUDE'] = df['LATITUDE83']
            df['SENDER LONGITUDE'] = df['LONGITUDE83']
            df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'EPA Handler ID', 'LATITUDE83', 'LONGITUDE83'], \
                        axis = 1, inplace = True)
            # Searching info for receiver
            df = pd.merge(df, RCRA, how = 'inner', left_on = \
                    'EPA ID Number of Facility to Which Waste was Shipped', right_on = 'PGM_SYS_ID')
            df['RECEIVER FRS ID'] = df['REGISTRY_ID']
            df['RECEIVER LATITUDE'] = df['LATITUDE83']
            df['RECEIVER LONGITUDE'] = df['LONGITUDE83']
            df['REPORTING YEAR'] = df['Reporting Cycle Year']
            df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'EPA ID Number of Facility to Which Waste was Shipped',\
                'LATITUDE83', 'LONGITUDE83', 'Reporting Cycle Year'], axis = 1, inplace = True)
            df = pd.merge(df, TRI, how = 'left', left_on = 'RECEIVER FRS ID', right_on = 'REGISTRY_ID')
            df['RECEIVER TRIFID'] = df['PGM_SYS_ID']
            # Translate management codes
            Path_WM = self._dir_path + '/../../Ancillary/RCRAInfo/RCRA_Management_Methods.csv'
            Management = pd.read_csv(Path_WM, header = 0, sep = ',', \
                                    usecols = ['Management Method Code', \
                                                'Management Method'])
            df = pd.merge(df, Management, how = 'left', on = ['Management Method Code'])
            df.rename(columns = {'Management Method': 'FOR WHAT IS TRANSFERRED',
                                'Waste Source Code': 'WASTE SOURCE CODE'},
                    inplace = True)
            df.drop(['REGISTRY_ID', 'PGM_SYS_ID', 'LATITUDE83', 'LONGITUDE83', \
                    'Management Method Code'], axis = 1, inplace = True)
            df = df[cols]
            df.drop_duplicates(keep = 'first', inplace = True)
            df.to_csv(self._dir_path + '/CSV/Off_site_tracking/RCRAInfo_' + self.year + '_Off-site_Tracking.csv', sep = ',',
                               index = False)


    def _Generating_SRS_Database(self, Database_name = ['TRI']):
        Dictionary_databases = {'TRI':'TRI_Chemical_List',
                                  'RCRA_T':'RCRA_T_Char_Characteristics_of_Hazardous_Waste_Toxicity_Characteristic',
                                  'RCRA_F':'RCRA_F_Waste_Hazardous_Wastes_From_Non-Specific_Sources',
                                  'RCRA_K':'RCRA_K_Waste_Hazardous_Wastes_From_Specific_Sources',
                                  'RCRA_P':'RCRA_P_Waste_Acutely_Hazardous_Discarded_Commercial_Chemical_Products',
                                  'RCRA_U':'RCRA_U_Waste_Hazardous_Discarded_Commercial_Chemical_Products'}
        path = self._dir_path  + '/../../Ancillary/Others'
        df_SRS = pd.DataFrame()
        for Schema in Database_name:
            df_db = pd.read_csv(path + '/' + Dictionary_databases[Schema] + '.csv',
                    usecols = ['ID', 'Internal Tracking Number', 'CAS'])
            df_db['Internal Tracking Number'] = df_db['Internal Tracking Number'].astype(pd.Int32Dtype())
            df_SRS = pd.concat([df_SRS, df_db], ignore_index = True,
                                  sort = True, axis = 0)
        return df_SRS


    def _Generating_FRS_Database(self, program):
        FSR_FACILITY = pd.read_csv(self._dir_path + '/../../Extract/FRS/CSV/NATIONAL_FACILITY_FILE.CSV',
                            low_memory = False,
                            dtype = {'POSTAL_CODE': 'object', 'REGISTRY_ID': 'int'},
                            usecols = ['REGISTRY_ID', 'LATITUDE83', 'LONGITUDE83'])
        FSR_FACILITY = FSR_FACILITY.drop_duplicates(subset = ['REGISTRY_ID'], keep = 'first')
        ENVIRONMENTAL_INTEREST = pd.read_csv(self._dir_path + '/../../Extract/FRS/CSV/NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV',
                            low_memory = False,
                            dtype = {'REGISTRY_ID': 'int'},
                            usecols = ['REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID'])
        ENVIRONMENTAL_INTEREST = ENVIRONMENTAL_INTEREST.drop_duplicates(keep = 'first')
        ENVIRONMENTAL_INTEREST = ENVIRONMENTAL_INTEREST.loc[ENVIRONMENTAL_INTEREST['PGM_SYS_ACRNM'].isin(program)]
        df_FRS = pd.merge(ENVIRONMENTAL_INTEREST, FSR_FACILITY, how = 'inner', on = 'REGISTRY_ID')
        return df_FRS


    def Joining_databases(self):
        Path_csv = self._dir_path + '/CSV/'
        Files = [File for File in os.listdir(Path_csv) if File.endswith('.csv')]
        Tracking = pd.DataFrame()
        for File in Files:
            Tracking_year = pd.read_csv(Path_csv + File, header = 0)
            Tracking = pd.concat([Tracking, Tracking_year], ignore_index = True, axis = 0)
        Tracking['Year_difference'] = Tracking.apply(lambda row: \
                abs(int(row['REPORTING YEAR']) - int(self.year[0])), axis = 1)
        grouping = ['SENDER FRS ID', 'SRS INTERNAL TRACKING NUMBER', 'CAS', 'RECEIVER FRS ID']
        Tracking = Tracking.loc[Tracking.groupby(grouping, as_index = False)\
                                    .Year_difference.idxmin()]
        Tracking.drop(['Year_difference', 'REPORTING YEAR'], axis = 1, inplace = True)
        Tracking.to_csv(self._dir_path + '/CSV/OUTPUT/Tracking_' + self.year[0] + '.csv', sep = ',',
                            index = False)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do?:\
                        [A]: Organize only one database\
                        [B]: Join all the databases',
                        type = str)

    parser.add_argument('-db', '--database', nargs = '?',
                        help = 'What database want to use (TRI or RCRAInfo)?.',
                        type = str,
                        default = None,
                        required = False)

    parser.add_argument('-Y', '--Year', nargs = '+',
                        help = 'What TRI or RCRAInfo year do you want to organize?.',
                        type = str,
                        required = True)


    args = parser.parse_args()

    if args.Option == 'A':

        for Y in args.Year:
            T = Off_Tracker(Y, args.database)
            T.retrieving_needed_information()

    elif args.Option == 'B':

        T = Off_Tracker(args.Year, args.database)
        T.Joining_databases()
