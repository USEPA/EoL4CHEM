# -*- coding: utf-8 -*-
#!/usr/bin/env python

# Received + Generated = Disposal + Released + Treated + Recycled + E_recovered + Transferred + Acumulated
# Received + Generated = Total Waste + Acumulated
# Assumption: a facility received to obtain value from the chemical o due to impurity
# Although FRS ID for off-site locations was added since 2018, we could observe that many facilities don´t know this entry and it easy to find this location using RCRA ID

# Importing libraries
import os, sys
import chardet, codecs
import argparse
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
from functools import reduce
import re
import warnings
import time
import math
warnings.simplefilter(action = 'ignore', category = FutureWarning)

from merging import *

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + '/../../extract/gps')
from gps_scraper import *

class TRI_EoL:

    def __init__(self, year, Files = None):
        self.year = year
        self.Files = Files
        self._dir_path = os.path.dirname(os.path.realpath(__file__)) # Working Directory
        #self._dir_path = os.getcwd() # if you are working on Jupyter Notebook


    def _dq_columns(self, file):
        Path = self._dir_path + '/../../ancillary/tri/TRI_File_' + file + '_Columns_for_DQ_Reliability.txt'
        inf_chardet = chardet.detect(open(Path, 'rb').read())
        inf_encoding = inf_chardet['encoding']
        file = codecs.open(Path, 'r', encoding = inf_encoding)
        Columns_for_scores = [Columns.rstrip().replace('\n','') for Columns in file.readlines()]
        file.close()
        return Columns_for_scores


    def _building_needed_dataframe(self, file):
        columns_converting = {'FACILITY ZIP CODE': lambda x:  '00' + x if len(x) == 3 else ('0' + x if len(x) == 4 else x), \
                            'REPORTING YEAR': lambda x: str(int(x)), \
                            'OFF-SITE ZIP CODE': lambda x:  '00' + x if len(x) == 3 else ('0' + x if len(x) == 4 else x), \
                            'CAS NUMBER': lambda x: x.lstrip('0'), \
                            'PRIMARY NAICS CODE': lambda x: str(int(x))}
        # Reading .txt with needed columns
        Path_txt = self._dir_path + '/../../ancillary/tri/TRI_File_' + file + '_needed_columns.txt'
        needed_columns = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:, 0].tolist()
        # Reading .csv
        Path_csv = self._dir_path + '/../../extract/tri/csv/US_' + file + '_' + self.year + '.csv'
        DataFrame = pd.read_csv(Path_csv, header = 0, sep = ',', low_memory = False,
                                converters = columns_converting, usecols = needed_columns)
        DataFrame.drop_duplicates(keep = 'first', inplace = True)
        DataFrame.sort_values(by = ['TRIFID', 'CAS NUMBER'], inplace = True)
        return DataFrame


    def _file(self, df):
        # NAICS Titles
        NAICS = pd.read_csv(self._dir_path + '/../../ancillary/others/NAICS_Structure.csv',
                            header = 0,
                            sep = ',',
                            converters = {'NAICS Title':lambda x: x.capitalize()},
                            dtype = {'NAICS Code': 'object'})
        NAICS.rename(columns = {'NAICS Title': 'GENERATOR TRI PRIMARY NAICS TITLE',
                                'NAICS Code': 'GENERATOR TRI PRIMARY NAICS CODE'},
                                inplace = True)
        # Dropping rows with 0ff-site transfers to facilities outside U.S. because they are facilities which are not subjected to TSCA
        df = df[pd.isnull(df['OFF-SITE COUNTRY ID'])]
        df.drop(['OFF-SITE COUNTRY ID'], axis = 1, inplace = True)
        # Checking if RCRA ID of off-site location has the correct structure
        df['OFF-SITE RCRA ID NR'] = df['OFF-SITE RCRA ID NR'].apply(lambda x: ''.join(re.findall(r'[A-Z0-9]+', str(x))))
        idx = df.loc[~df['OFF-SITE RCRA ID NR'].str.contains('\s?[A-Z]{2,3}[0-9]{8,9}\s?',  na = False)].index.tolist()
        df['OFF-SITE RCRA ID NR'].loc[idx] = None
        # Changing the units of measure to kg
        Cols_Transfer = df.columns[24::2].tolist()
        df[Cols_Transfer].fillna(0.0, inplace = True)
        df.loc[df['UNIT OF MEASURE'] == 'Pounds', Cols_Transfer] *= 0.453592
        df.loc[df['UNIT OF MEASURE'] == 'Grams', Cols_Transfer] *= 10**-3
        df[Cols_Transfer] = df[Cols_Transfer].round(6)
        df['UNIT OF MEASURE'] = 'kg'
        # Renaming
        df.rename(columns = {'TRIFID': 'GENERATOR TRIFID', 'FACILITY NAME': 'GENERATOR NAME', 'FACILITY STREET': 'GENERATOR STREET', \
                            'FACILITY CITY': 'GENERATOR CITY', 'FACILITY COUNTY': 'GENERATOR COUNTY', 'FACILITY STATE': 'GENERATOR STATE', \
                            'FACILITY ZIP CODE': 'GENERATOR ZIP', 'CAS NUMBER': 'TRI CHEMICAL ID NUMBER', 'CHEMICAL NAME':  'TRI CHEMICAL NAME', \
                            'CLASSIFICATION': 'TRI CLASSIFICATION', 'UNIT OF MEASURE': 'UNIT', 'PRIMARY NAICS CODE': 'GENERATOR TRI PRIMARY NAICS CODE', \
                            'OFF-SITE RCRA ID NR': 'RECEIVER RCRAInfo ID', 'OFF-SITE NAME': 'RECEIVER NAME', 'OFF-SITE STREET ADDRESS': 'RECEIVER STREET', \
                            'OFF-SITE CITY': 'RECEIVER CITY', 'OFF-SITE COUNTY': 'RECEIVER COUNTY', 'OFF-SITE STATE': 'RECEIVER STATE', \
                            'OFF-SITE ZIP CODE': 'RECEIVER ZIP', 'LATITUDE': 'GENERATOR LATITUDE', 'LONGITUDE': 'GENERATOR LONGITUDE'}
                , inplace = True)
        # Organizing flows
        Cols_Facility = df.iloc[:, 0:24].columns.tolist()
        TRI = pd.DataFrame()
        for Col  in Cols_Transfer:
            df_aux = df[Cols_Facility + [Col, Col + ' - BASIS OF ESTIMATE']]
            df_aux.rename(columns = {Col: 'QUANTITY TRANSFER OFF-SITE',
                                    Col + ' - BASIS OF ESTIMATE': 'RELIABILITY OF OFF-SITE TRANSFER'},
                        inplace = True)
            df_aux['OFF-SITE WASTE MANAGEMENT'] = re.sub(r'potws', 'POTWS', re.sub(r'Rcra|rcra', 'RCRA', Col.replace('OFF-SITE - ', '').strip().capitalize()))
            TRI = pd.concat([TRI, df_aux], ignore_index = True,
                                  sort = True, axis = 0)
        TRI =  TRI.loc[TRI['QUANTITY TRANSFER OFF-SITE'] != 0.0]
        TRI['RELIABILITY OF OFF-SITE TRANSFER'].fillna('5', inplace = True)
        TRI = pd.merge(TRI, NAICS, on = 'GENERATOR TRI PRIMARY NAICS CODE',
                        how = 'left')
        # Reading .txt with order for columns
        Path_txt = self._dir_path + '/../../ancillary/Features_at_EoL.txt'
        Columns = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:, 0].tolist()
        Columns = [col for col in Columns if col in TRI.columns]
        TRI = TRI[columns]
        return TRI


    def generate_dataframe(self):
        regex =  re.compile(r'TRI_File_(\d{1}[a-zA-Z]?)_Columns_for_DQ_Reliability.txt')
        Path_DQ = self._dir_path + '/../../ancillary/tri'
        File_DQ = [re.search(regex,file).group(1) for file in os.listdir(Path_DQ) if re.match(regex, file)]
        TRI_needed = {}
        for key in self.Files:
            TRI_needed[key] = self._building_needed_dataframe(key) # Loading dataframe with columns needed
            if key == '1b':
                cols = TRI_needed[key].columns.tolist()
                func = fuctions_rows_grouping(TRI_needed[key])
                TRI_needed[key] = TRI_needed[key].groupby(['TRIFID', 'CAS NUMBER'],
                                                as_index = False) \
                                                .agg(func)
                TRI_needed[key] = TRI_needed[key][cols]
                Cols_CoU = TRI_needed[key].iloc[:, 13:24].columns.tolist()
                TRI_needed[key]['GENERATOR CONDITION OF USE'] = TRI_needed[key][Cols_CoU].apply(lambda row: ' + '.join(val.capitalize() \
                                                                    for val in Cols_CoU if row[val].strip() == 'YES'), axis = 1)
                TRI_needed[key].drop(columns = Cols_CoU, inplace = True)
            if key != '1b':
                mapping = {'M': '1', 'M1': '1', 'M2': '1', 'E': '2',
                        'E1': '2', 'E2': '2', 'C': '3', 'O': '4',
                        'X': '5', 'N':'5', 'NA':'5'}
                Columns_for_scores = self._dq_columns(key) # Loading columns for data quality score
                TRI_needed[key][Columns_for_scores] = TRI_needed[key][Columns_for_scores].apply(lambda x: x.str.strip().map(mapping), axis = 1)
        # Joining TRI databases based on CAS Number, TRI Facility ID, and elemental metal included (for metal compounds)
        TRI_merged = reduce(lambda  left, right: pd.merge(left , right,
                        on = ['TRIFID', 'CAS NUMBER'],
                        how = 'inner'), list(TRI_needed.values()))
        TRI_merged.drop_duplicates(keep = 'first', inplace = True)
        del TRI_needed
        TRI_EoL = self._file(TRI_merged)
        if not os.path.isdir(self._dir_path + '/' + self.year):
            os.mkdir(self._dir_path + '/' + self.year)
        TRI_EoL.to_csv(self._dir_path + '/' + self.year + '/TRI_' + self.year + '_EoL.csv', sep = ',',
                        index = False)


    def _generating_srs_database(self, Database_name = ['TRI'], \
                                columns = ['CAS', 'ID', 'Internal Tracking Number']):
        Dictionary_databases = {'TRI':'TRI_Chemical_List',
                              'RCRA_T':'RCRA_T_Char_Characteristics_of_Hazardous_Waste_Toxicity_Characteristic',
                              'RCRA_F':'RCRA_F_Waste_Hazardous_Wastes_From_Non-Specific_Sources',
                              'RCRA_K':'RCRA_K_Waste_Hazardous_Wastes_From_Specific_Sources',
                              'RCRA_P':'RCRA_P_Waste_Acutely_Hazardous_Discarded_Commercial_Chemical_Products',
                              'RCRA_U':'RCRA_U_Waste_Hazardous_Discarded_Commercial_Chemical_Products',
                              'CAA_HAP':'CAA_Hazardous_Air_Pollutants',
                              'CAA_VOC':'CAA_National_Volatile_Organic_Compound_Emission_Standards',
                              'CWA_Biosolids':'CWA_List_of_Pollutants_Identified_in_Biosolids',
                              'CWA_Priority':'CWA_Priority_Pollutant_List',
                              'SDWA_Candidate':'SDWA_Contaminant_Candidate_List',
                              'SDWA_NPDWR':'SDWA_National_Primary_Drinking_Water_Regulations',
                              'TSCA_NC_Inventory':'TSCA_Nonconfidential_Inventory'}
        path = self._dir_path  + '/../../ancillary/others'
        df_SRS = pd.DataFrame()
        for Schema in Database_name:
            df_name = [Schema]
            if Schema == 'CAA_HAP':
                df_name.append('CAA_VOC')
            df = pd.DataFrame()
            for name in df_name:
                df_db = pd.read_csv(path + '/' + Dictionary_databases[name] + '.csv',
                                    usecols = columns)
                df_db['Internal Tracking Number'] = df_db['Internal Tracking Number'].astype(pd.Int32Dtype())
                df = pd.concat([df, df_db], ignore_index = True,
                                      sort = True, axis = 0)
            df.drop_duplicates(keep = 'first', inplace = True)
            df['Source'] = Schema
            df_SRS = pd.concat([df_SRS, df], ignore_index = True,
                              sort = True, axis = 0)
        return df_SRS


    def srs_search(self):
        # Searchin in regulations
        DB_to_search = ['TRI', 'CAA_HAP', 'CWA_Biosolids', 'CWA_Priority', \
                        'SDWA_Candidate', 'SDWA_NPDWR', 'TSCA_NC_Inventory']
        Regulations_SRS = self._generating_srs_database(Database_name = DB_to_search)
        Chemical_SRS_TRI = Regulations_SRS[Regulations_SRS['Source'] == 'TRI']
        TRI = pd.read_csv(self._dir_path + '/' + self.year + '/TRI_' + self.year + '_EoL.csv',
                        sep = ',', header = 0,
                        dtype = {'REPORTING YEAR':'int',
                                'GENERATOR ZIP':'object',
                                'RECEIVER ZIP':'object'})
        Columns = list(TRI.columns.values)
        TRI_CHEMICAL_IDS = list(TRI['TRI CHEMICAL ID NUMBER'].unique())
        SRS_TRI = Chemical_SRS_TRI.loc[Chemical_SRS_TRI['ID']. \
                                              isin(TRI_CHEMICAL_IDS)].drop_duplicates(keep = 'first')
        SRS_TRI.drop('Source', inplace = True, axis = 1)
        del Chemical_SRS_TRI
        ITN = list(SRS_TRI['Internal Tracking Number'].unique())
        Regulations_SRS = Regulations_SRS[Regulations_SRS['Source'] != 'TRI']
        SRS_Reg_only = Regulations_SRS.loc[Regulations_SRS['Internal Tracking Number']. \
                                              isin(ITN)].drop_duplicates(keep = 'first')
        SRS_Reg_only.drop(['ID', 'CAS'], inplace = True, axis = 1)
        del Regulations_SRS
        regulations = list(SRS_Reg_only['Source'].unique())
        for reg in regulations:
            TRI_reg = SRS_Reg_only.loc[SRS_Reg_only['Source'] == reg]
            SRS_TRI[reg.replace('_', ' ').upper() + '?'] = 'No'
            SRS_TRI = pd.merge(SRS_TRI, TRI_reg, how = 'left', on = 'Internal Tracking Number')
            SRS_TRI.loc[SRS_TRI['Source'] == reg, reg.replace('_', ' ').upper() + '?'] = 'Yes'
            SRS_TRI.drop('Source', inplace = True, axis = 1)
        # Searching in RCRAInfo
        ## Note: it is possible than some value duplicate due to a chemical may belongs to different list
        ## of RCRA listing and characteristic waste. The above is useful for the tracking and data augmentation (ML)
        Chemical_SRS_RCRA = self._generating_srs_database(Database_name = ['RCRA_T', \
                             'RCRA_F', 'RCRA_K', 'RCRA_P', 'RCRA_U'], columns = ['ID', \
                             'Internal Tracking Number'])
        Chemical_SRS_RCRA.drop('Source', inplace = True, axis = 1)
        RCRA_TRI = Chemical_SRS_RCRA.loc[Chemical_SRS_RCRA['Internal Tracking Number']. \
                                        isin(ITN)].drop_duplicates(keep = 'first')
        SRS_TRI_RCRA = pd.merge(SRS_TRI, RCRA_TRI, how = 'left', on = 'Internal Tracking Number')
        SRS_TRI_RCRA['SRS CHEMICAL ID'] = SRS_TRI_RCRA['Internal Tracking Number']
        SRS_TRI_RCRA['RCRAInfo CHEMICAL ID NUMBER'] = SRS_TRI_RCRA['ID_y']
        SRS_TRI_RCRA['CAS NUMBER'] = SRS_TRI_RCRA['CAS']
        SRS_TRI_RCRA['TRI CHEMICAL ID NUMBER'] = SRS_TRI_RCRA['ID_x']
        SRS_TRI_RCRA.drop(['ID_x', 'ID_y', 'Internal Tracking Number', 'CAS'], axis = 1,
                                          inplace = True)
        TRI_RCRAInfo_Merged = pd.merge(TRI, SRS_TRI_RCRA, how = 'left', on = 'TRI CHEMICAL ID NUMBER')
        SRS_TRI_Columns = list(SRS_TRI_RCRA.columns)
        # Reading .txt with order for columns
        Path_txt = self._dir_path + '/../../ancillary/Features_at_EoL.txt'
        Columns = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:, 0].tolist()
        Columns = [col for col in Columns if col in TRI_RCRAInfo_Merged.columns]
        TRI_RCRAInfo_Merged = TRI_RCRAInfo_Merged[Columns].drop_duplicates(keep = 'first')
        TRI_RCRAInfo_Merged.to_csv(self._dir_path + '/' + self.year + '/TRI_SRS_' + self.year + '_EoL.csv',
                                    sep = ',', index = False)


    def _name_comparison(self, Name_FRS, Name_TRI):
        try:
            # Organizing names for comparison
            Name_FRS = ' '.join(re.findall(r'[a-zA-Z0-9]+', str(Name_FRS).replace('.', '').upper()))
            Name_TRI = ' '.join(re.findall(r'[a-zA-Z0-9]+', str(Name_TRI).replace('.', '').upper()))
            if (Name_FRS in Name_TRI) | (Name_TRI in Name_FRS):
                return True
            else:
                return False
        except TypeError:
            return False


    def _address_comparison(self, Address_FRS, Address_TRI):
        # https://pe.usps.com/text/pub28/28apc_002.htm
        # http://www.gis.co.clay.mn.us/USPS.htm#w
        ordinal = lambda n: "%d%s" % (int(n), 'TSNRHTDD'[(math.floor(int(n)/10)%10!=1)*(int(n)%10<4)*int(n)%10::4]) if re.search(r'^[0-9]+$', n) else n
        df_abbreviations = pd.read_csv(self._dir_path  + '/../../ancillary/others/Official_USPS_Abbreviations.csv')
        abbreviations = {row['Commonly_Used_Street_Suffix_or_Abbreviation']: row['Primary_Street'] for idx, row in df_abbreviations.iterrows()}
        abbreviations.update({row['Postal_Service_Standard_Suffix_Abbreviation']: row['Primary_Street'] for idx, row in df_abbreviations.iterrows() \
                             if not row['Postal_Service_Standard_Suffix_Abbreviation'] in abbreviations.keys()})
        del df_abbreviations
        try:
            # Organizing address for comparison
            Address_FRS = ' '.join(ordinal(word) if word not in abbreviations.keys() else abbreviations[word] \
                        for word in re.findall(r'[a-zA-Z0-9]+', str(Address_FRS).replace('.', '').upper()))
            Address_TRI = ' '.join(ordinal(word) if word not in abbreviations.keys() else abbreviations[word] \
                        for word in re.findall(r'[a-zA-Z0-9]+', str(Address_TRI).replace('.', '').upper()))
            if (Address_TRI in Address_FRS) | (Address_FRS in Address_TRI):
                return True
            else:
                return False
        except TypeError:
            return False


    def frs_search(self):
        # Calling database
        TRI = pd.read_csv(self._dir_path + '/' + self.year + '/TRI_SRS_' + self.year + '_EoL.csv',
                        sep = ',', header = 0,
                        dtype = {'REPORTING YEAR':'int',
                                'GENERATOR ZIP':'object',
                                'RECEIVER ZIP':'object',
                                'GENERATOR TRI PRIMARY NAICS CODE':'object',
                                'SRS CHEMICAL ID NUMBER':'object'})
        TRI['RECEIVER ZIP'] = TRI['RECEIVER ZIP'].apply(lambda code: str(code[0:5]) if str(code) and len(str(code)) >= 5 else None)
        Columns = list(TRI.columns)
        # Separating between records with off-site facilities which have RCRA ID and others than
        TRI_non_IDs = TRI.loc[~pd.notnull(TRI['RECEIVER RCRAInfo ID'])]
        TRI_IDs = TRI.loc[pd.notnull(TRI['RECEIVER RCRAInfo ID'])]
        del TRI
        # Calling FRS
        FRS_FACILITY = pd.read_csv(self._dir_path + '/../../extract/frs/csv/NATIONAL_FACILITY_FILE.CSV',
                            low_memory = False,
                            dtype = {'POSTAL_CODE': 'object', 'REGISTRY_ID': 'int'},
                            usecols = ['REGISTRY_ID', 'PRIMARY_NAME', 'LOCATION_ADDRESS',
                                    'CITY_NAME', 'COUNTY_NAME', 'STATE_CODE', 'POSTAL_CODE',
                                    'LATITUDE83', 'LONGITUDE83'])
        FRS_FACILITY['POSTAL_CODE'] = FRS_FACILITY['POSTAL_CODE'].apply(lambda code: str(code[0:5]) if str(code) and len(str(code)) >= 5 else None)
        FRS_FACILITY = FRS_FACILITY.drop_duplicates(keep = 'first')
        ENVIRONMENTAL_INTEREST = pd.read_csv(self._dir_path + '/../../extract/frs/csv/NATIONAL_ENVIRONMENTAL_INTEREST_FILE.CSV',
                            low_memory = False,
                            dtype = {'REGISTRY_ID': 'int'},
                            usecols = ['REGISTRY_ID', 'PGM_SYS_ACRNM', 'PGM_SYS_ID'])
        E_RCRAINFO = ENVIRONMENTAL_INTEREST[ENVIRONMENTAL_INTEREST['PGM_SYS_ACRNM'] == 'RCRAINFO']
        E_RCRAINFO.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
        E_RCRAINFO['REGISTRY_ID'] = E_RCRAINFO['REGISTRY_ID'].apply(lambda x: abs(x))
        E_TRI = ENVIRONMENTAL_INTEREST[ENVIRONMENTAL_INTEREST['PGM_SYS_ACRNM'] == 'TRIS']
        E_TRI.drop('PGM_SYS_ACRNM', axis = 1, inplace = True)
        E_TRI['REGISTRY_ID'] = E_TRI['REGISTRY_ID'].apply(lambda x: abs(x))
        # Searching information for facilities without RCRA ID
        RECEIVER_FACILITY = TRI_non_IDs[['RECEIVER NAME', 'RECEIVER STREET', 'RECEIVER CITY', \
                                'RECEIVER COUNTY', 'RECEIVER STATE', 'RECEIVER ZIP']] \
                                .drop_duplicates(keep = 'first')
        # Searching by city, county, state and zip
        df1 = pd.merge(RECEIVER_FACILITY, FRS_FACILITY, how = 'left',
                        left_on = ['RECEIVER CITY','RECEIVER COUNTY', 'RECEIVER STATE',
                        'RECEIVER ZIP'],
                        right_on = ['CITY_NAME', 'COUNTY_NAME', 'STATE_CODE',
                        'POSTAL_CODE']).drop_duplicates(keep = 'first')
        # Searching by name
        df2 = df1[df1.apply(lambda x:  self._name_comparison(x['PRIMARY_NAME'], x['RECEIVER NAME']), axis = 1)] \
                            .drop_duplicates(keep = 'first')
        # Searching by address
        df3 = df2[df2.apply(lambda x:  self._address_comparison(x['LOCATION_ADDRESS'], x['RECEIVER STREET']),\
                        axis = 1)].drop_duplicates(keep = 'first')
        df3['RECEIVER FRS ID'] = df3['REGISTRY_ID'].astype(int).apply(lambda x: abs(x))
        df3.drop(['REGISTRY_ID', 'PRIMARY_NAME', 'LOCATION_ADDRESS',
                'CITY_NAME', 'COUNTY_NAME', 'STATE_CODE', 'POSTAL_CODE'],
                axis = 1, inplace = True)
        TRI_non_IDs = pd.merge(TRI_non_IDs, df3, on = ['RECEIVER NAME', 'RECEIVER STREET', 'RECEIVER CITY', \
                            'RECEIVER COUNTY', 'RECEIVER STATE', 'RECEIVER ZIP'], how = 'right').drop_duplicates(keep = 'first')
        # Searching information for facilities with RCRA ID
        TRI_IDs = pd.merge(TRI_IDs, E_RCRAINFO, left_on = 'RECEIVER RCRAInfo ID', \
                                    right_on = 'PGM_SYS_ID', how = 'inner').drop_duplicates(keep = 'first')
        TRI_IDs = pd.merge(TRI_IDs, FRS_FACILITY[['REGISTRY_ID', 'LATITUDE83', 'LONGITUDE83']],
                           on = 'REGISTRY_ID', how = 'inner')
        TRI_IDs['RECEIVER FRS ID'] = TRI_IDs['REGISTRY_ID'].astype(int).apply(lambda x: abs(x))
        TRI_IDs.drop(['REGISTRY_ID', 'PGM_SYS_ID'], axis = 1, inplace = True)
        TRI_FRS = pd.concat([TRI_IDs, TRI_non_IDs], ignore_index = True,
                             sort = True, axis = 0)
        # Searching facilities in RCRAInfo (without RCRA ID)
        TRI_RCRAInfo = pd.merge(E_RCRAINFO, TRI_non_IDs,
                                 left_on = 'REGISTRY_ID',
                                 right_on = 'RECEIVER FRS ID',
                                 how = 'right')
        TRI_RCRAInfo['RECEIVER RCRAInfo ID'] = TRI_RCRAInfo['PGM_SYS_ID']
        TRI_RCRAInfo.drop(['REGISTRY_ID', 'PGM_SYS_ID'], axis = 1, inplace = True)
        TRI_RCRAInfo = TRI_RCRAInfo.drop_duplicates(keep = 'first')
        TRI_RCRAInfo = pd.concat([TRI_IDs, TRI_RCRAInfo], ignore_index = True,
                             sort = True, axis = 0)
        # Searching facilities in TRI
        TRI_RCRAInfo_TRI = pd.merge(E_TRI, TRI_RCRAInfo,
                                left_on = 'REGISTRY_ID',
                                 right_on = 'RECEIVER FRS ID',
                                 how = 'right')
        TRI_RCRAInfo_TRI['RECEIVER TRIFID'] = TRI_RCRAInfo_TRI['PGM_SYS_ID']
        TRI_RCRAInfo_TRI = TRI_RCRAInfo_TRI[pd.notnull(TRI_RCRAInfo_TRI['RECEIVER RCRAInfo ID']) \
                                | pd.notnull(TRI_RCRAInfo_TRI['RECEIVER TRIFID'])]
        TRI_RCRAInfo_TRI.drop(['REGISTRY_ID', 'PGM_SYS_ID'], axis = 1, inplace = True)
        TRI_RCRAInfo_TRI.rename(columns = {'LATITUDE83': 'RECEIVER LATITUDE',
                                           'LONGITUDE83': 'RECEIVER LONGITUDE'},
                                inplace = True)
        # Reading .txt with order for columns
        Path_txt = self._dir_path + '/../../ancillary/Features_at_EoL.txt'
        Columns = pd.read_csv(Path_txt, header = None, sep = '\t').iloc[:, 0].tolist()
        Columns = [col for col in Columns if col in TRI_RCRAInfo_TRI.columns]
        TRI_RCRAInfo_TRI.drop_duplicates(keep = 'first', inplace = True)
        # Searching lat and long for receivers
        NON_LAT_LONG = TRI_RCRAInfo_TRI.loc[pd.isnull(TRI_RCRAInfo_TRI['RECEIVER LATITUDE']),
                                        ['RECEIVER FRS ID', 'RECEIVER STREET',
                                        'RECEIVER CITY', 'RECEIVER STATE',
                                        'RECEIVER ZIP']]\
                                        .drop_duplicates(keep = 'first')
        NON_LAT_LONG.rename(columns = {'RECEIVER STREET': 'ADDRESS',
                                       'RECEIVER CITY': 'CITY',
                                       'RECEIVER STATE': 'STATE',
                                       'RECEIVER ZIP': 'ZIP'},
                            inplace = True)
        Scraper = GPS_scraper(NON_LAT_LONG)
        NON_LAT_LONG = Scraper.browsing()
        TRI_RCRAInfo_TRI = pd.merge(TRI_RCRAInfo_TRI,
                                    NON_LAT_LONG[['RECEIVER FRS ID', 'LONGITUDE', 'LATITUDE']],
                                    on = 'RECEIVER FRS ID', how = 'inner')
        idx = TRI_RCRAInfo_TRI.loc[pd.isnull(TRI_RCRAInfo_TRI['RECEIVER LATITUDE'])].index.tolist()
        TRI_RCRAInfo_TRI.loc[idx, 'RECEIVER LATITUDE'] = TRI_RCRAInfo_TRI.loc[idx, 'LATITUDE']
        TRI_RCRAInfo_TRI.loc[idx, 'RECEIVER LONGITUDE'] = TRI_RCRAInfo_TRI.loc[idx, 'LONGITUDE']
        TRI_RCRAInfo_TRI.drop(columns = ['LONGITUDE', 'LATITUDE'], inplace = True)
        # Saving
        TRI_RCRAInfo_TRI = TRI_RCRAInfo_TRI[Columns]
        print(TRI_RCRAInfo_TRI.info())
        TRI_RCRAInfo_TRI.to_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_' + self.year + '_EoL.csv',
                             sep = ',', index = False)


    def _Off_tracker(self, Management, Receiver_FRS_ID, Chemical_SRS_ID, RCRA_ID, Track, df_WM):
        WM = list(df_WM.loc[df_WM['TRI Waste Management'] == Management.strip(), 'RCRA Waste Management'].unique())
        Brokerage = 'Storage and Transfer -The site receiving this waste stored/bulked and transferred the waste with no reclamation, recovery, destruction, treatment, or disposal at that site'
        Blending = 'Fuel blending prior to energy recovery at another site (waste generated on-site or received from off-site)'
        WM.append(Brokerage)
        # With chemical of concern
        Track = Track.loc[Track['SRS INTERNAL TRACKING NUMBER'] == Chemical_SRS_ID]
        Track.drop(columns = 'SRS INTERNAL TRACKING NUMBER', inplace = True)
        Quantity_transferred = Track.loc[pd.notnull(Track['QUANTITY TRANSFERRED'])]
        Quantity_transferred.drop(columns =  'QUANTITY RECEIVED', inplace = True)
        Quantity_transferred =  Quantity_transferred.loc[Quantity_transferred['FOR WHAT IS TRANSFERRED'].isin(WM)]  # Considering waste management
        Quantity = Quantity_transferred
        grouping = ['FOR WHAT IS TRANSFERRED', 'RECEIVER FRS ID', 'GENERATOR FRS ID']
        if RCRA_ID:
            Quantity_received = Track.loc[pd.notnull(Track['QUANTITY RECEIVED'])]
            Quantity_received.drop(columns =  ['QUANTITY TRANSFERRED', 'RECEIVER TRIFID', 'GENERATOR FRS ID', 'RELIABILITY'], inplace = True)
            Quantity_received =  Quantity_received.loc[Quantity_received['FOR WHAT IS TRANSFERRED'].isin(WM)] # Considering waste management
            Quantity = pd.merge(Quantity_transferred, Quantity_received, how = 'left',
                                    on = ['FOR WHAT IS TRANSFERRED', 'RECEIVER FRS ID'])
            Quantity_null = Quantity.loc[pd.isnull(Quantity['QUANTITY RECEIVED'])]
            Quantity_null['Year_difference'] = Quantity_null.apply(lambda row:
                        abs(row['REPORTING YEAR_x'] - int(self.year)), \
                         axis = 1)
            Quantity = Quantity.loc[pd.notnull(Quantity['QUANTITY RECEIVED'])]
            Quantity = Quantity.loc[Quantity['QUANTITY RECEIVED'] >= Quantity['QUANTITY TRANSFERRED']]
            Quantity['Year_difference'] = Quantity.apply(lambda row:
                        0.5*(abs(row['REPORTING YEAR_x'] - int(self.year)) + abs(int(self.year) - row['REPORTING YEAR_y'])), \
                         axis = 1)
            Quantity = pd.concat([Quantity, Quantity_null], ignore_index = True, axis = 0)
            del Quantity_null, Quantity_received
            Quantity = Quantity.loc[Quantity.groupby(grouping, as_index = False)\
                                             .Year_difference.idxmin()] # Selecting between years
            Quantity.drop(columns = ['REPORTING YEAR_x', 'REPORTING YEAR_y', 'QUANTITY RECEIVED'], inplace = True)
        else:
            Quantity['Year_difference'] = Quantity.apply(lambda row:
                        abs(row['REPORTING YEAR'] - int(self.year)), \
                         axis = 1)
            Quantity = Quantity.loc[Quantity.groupby(grouping, as_index = False)\
                                             .Year_difference.idxmin()] # Selecting between years
            Quantity.drop(columns = ['REPORTING YEAR'], inplace = True)
        del Quantity_transferred, Track
        Source = Quantity.loc[Quantity['GENERATOR FRS ID'] == Receiver_FRS_ID]
        if not Source.empty:
            Source['MAXIMUM POSSIBLE FLOW'] = Source['QUANTITY TRANSFERRED']
            Source['MAXIMUM POSSIBLE FLOW RELIABILITY'] = Source['RELIABILITY']
            Source['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW'] = Source.apply(lambda row: \
                                                    self._temporal_correlation(row['Year_difference']),
                                                    axis = 1)
            Source.drop(columns = ['QUANTITY TRANSFERRED', 'GENERATOR FRS ID', 'RELIABILITY', 'Year_difference'], inplace = True)
            if Management == 'Transfer to waste broker for energy recovery':
                Paths = Source.loc[~Source['FOR WHAT IS TRANSFERRED'].isin([Brokerage, Blending])]
            else:
                Paths = Source.loc[Source['FOR WHAT IS TRANSFERRED'] != Brokerage]
            Paths.rename(columns = {'RECEIVER FRS ID': 'RETDF FRS ID',
                                        'RECEIVER TRIFID': 'RETDF TRIFID'},
                            inplace = True)
            if Management == 'Transfer to waste broker for energy recovery':
                Source = Source.loc[Source['FOR WHAT IS TRANSFERRED'].isin([Brokerage, Blending])]
            else:
                Source = Source.loc[Source['FOR WHAT IS TRANSFERRED'] == Brokerage]
            n = 0
            while (not Source.empty) and (n < 6):
                n = n + 1
                Source.drop(columns = ['FOR WHAT IS TRANSFERRED', 'RECEIVER TRIFID'],
                                inplace = True)
                Source.rename(columns = {'RECEIVER FRS ID': 'GENERATOR FRS ID'}, inplace =  True)
                Source = pd.merge(Source, Quantity, how = 'inner', on = 'GENERATOR FRS ID')
                Source.drop_duplicates(keep = 'first', inplace = True)
                try:
                    Source['MAXIMUM POSSIBLE FLOW'] = Source.apply(lambda row: \
                                                            min([row['MAXIMUM POSSIBLE FLOW'],
                                                                row['QUANTITY TRANSFERRED']]),
                                                            axis = 1)
                    Source['MAXIMUM POSSIBLE FLOW RELIABILITY'] = Source.apply(lambda row: \
                                                            max([row['RELIABILITY'],
                                                                row['MAXIMUM POSSIBLE FLOW RELIABILITY']]),
                                                            axis = 1)
                    Source['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW'] = Source.apply(lambda row: \
                                                            max([self._temporal_correlation(row['Year_difference']), \
                                                            row['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW']]),
                                                            axis =  1)
                    Source.drop(columns = ['QUANTITY TRANSFERRED', 'GENERATOR FRS ID', 'RELIABILITY', 'Year_difference'], inplace = True)
                    if Management == 'Transfer to waste broker for energy recovery':
                        Paths_aux = Source.loc[~Source['FOR WHAT IS TRANSFERRED'].isin([Brokerage, Blending])]
                    else:
                        Paths_aux = Source.loc[Source['FOR WHAT IS TRANSFERRED'] != Brokerage]
                    if not Paths_aux.empty:
                        Paths_aux.rename(columns = {'RECEIVER FRS ID': 'RETDF FRS ID',
                                                'RECEIVER TRIFID': 'RETDF TRIFID'},
                                    inplace = True)
                        Paths = pd.concat([Paths, Paths_aux], ignore_index = True, axis = 0)
                    if Management == 'Transfer to waste broker for energy recovery':
                        Source = Source.loc[Source['FOR WHAT IS TRANSFERRED'].isin([Brokerage, Blending])]
                    else:
                        Source = Source.loc[Source['FOR WHAT IS TRANSFERRED'] == Brokerage]
                except ValueError:
                    n = 6
            Paths.drop_duplicates(keep = 'first', inplace = True)
            Paths = Paths.loc[pd.notnull(Paths['RETDF TRIFID'])]
            Paths['OFF-SITE WASTE MANAGEMENT'] = Management
            Paths['RECEIVER FRS ID'] = Receiver_FRS_ID
            Paths['SRS CHEMICAL ID'] = Chemical_SRS_ID
            Paths['RCRAInfo CHEMICAL ID NUMBER'] = RCRA_ID
            df_WM_aux = df_WM.rename(columns = {'RCRA Waste Management': 'FOR WHAT IS TRANSFERRED',
                        'General': 'GENERAL WASTE MANAGEMENT',
                        'Type of waste management': 'TYPE OF WASTE MANAGEMENT',
                        'TRI Waste Management': 'OFF-SITE WASTE MANAGEMENT'})
            Paths = pd.merge(Paths, df_WM_aux, how = 'left', on = ['OFF-SITE WASTE MANAGEMENT', 'FOR WHAT IS TRANSFERRED'])
            del df_WM_aux
            Paths.drop(columns = ['FOR WHAT IS TRANSFERRED'], inplace = True)
            Paths.drop_duplicates(keep = 'first', inplace = True)
            return Paths
        else:
            return pd.DataFrame()


    def _temporal_correlation(self, difference):
        if difference <= 3:
            return 1
        elif difference > 3 and difference <= 6:
            return 2
        elif difference > 6 and difference <= 10:
            return 3
        elif difference > 10 and difference <= 15:
            return 4
        else:
            return 5


    def flows_search(self):
        # Calling database
        TRI = pd.read_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_' + self.year + '_EoL.csv',
                                  header = 0, sep = ',', low_memory = False)
        columns_grouping = TRI.iloc[:,0:31].columns.tolist()
        cols = TRI.columns.tolist()
        # Brokerage?
        Broker = TRI.loc[TRI['OFF-SITE WASTE MANAGEMENT'].str.contains('broker',  na = False)]
        No_broker = TRI.loc[~TRI['OFF-SITE WASTE MANAGEMENT'].str.contains('broker',  na = False)]
        del TRI
        #------------------------------- No brokers -------------------------------#
        # The off-site facility is RCRA?
        RCRA_facility = No_broker.loc[pd.notnull(No_broker['RECEIVER RCRAInfo ID'])]
        TRI_facility = No_broker.loc[pd.isnull(No_broker['RECEIVER RCRAInfo ID'])]
        del No_broker
        # Checking the EPCRA chemical is RCRA hazardous waste
        non_hazardous = RCRA_facility.loc[pd.isnull(RCRA_facility['RCRAInfo CHEMICAL ID NUMBER']) \
                                         & pd.notnull(RCRA_facility['RECEIVER TRIFID'])]
        RCRA_facility = RCRA_facility.loc[pd.notnull(RCRA_facility['RCRAInfo CHEMICAL ID NUMBER'])]
        TRI_facility = pd.concat([TRI_facility, non_hazardous], ignore_index = True, axis = 0)
        del non_hazardous
        # Searching information for RCRA facilities
        Path_WM = self._dir_path + '/../../ancillary/Others/TRI_RCRA_Management_Match.csv'
        Management = pd.read_csv(Path_WM, header = 0, sep = ',')
        Management.drop_duplicates(keep = 'first', inplace = True)
        RCRA_facility = pd.merge(RCRA_facility, Management, how = 'left', left_on = 'OFF-SITE WASTE MANAGEMENT', \
                            right_on = 'TRI Waste Management').drop_duplicates(keep = 'first')
        Path_RCRA = self._dir_path + '/../Waste_Tracking/CSV/Off_site_tracking/'
        Files = [File for File in os.listdir(Path_RCRA) if File.startswith('RCRAInfo')]
        RCRAINFO = pd.DataFrame()
        for F in Files:
            r = pd.read_csv(Path_RCRA + F, header = 0, sep = ',', low_memory = False,
                    usecols = ['REPORTING YEAR', 'SRS INTERNAL TRACKING NUMBER',
                        'QUANTITY RECEIVED', 'FOR WHAT IS TRANSFERRED',
                        'RECEIVER TRIFID', 'RECEIVER FRS ID'])
            r = r.loc[(r['QUANTITY RECEIVED'] != 0) & (pd.notnull(r['QUANTITY RECEIVED']))]
            r.rename(columns = {'SRS INTERNAL TRACKING NUMBER': 'SRS CHEMICAL ID',
                                'FOR WHAT IS TRANSFERRED': 'RCRA Waste Management',
                                'REPORTING YEAR': 'RECEIVING YEAR',
                                'RECEIVER FRS ID': 'RETDF FRS ID',
                                'RECEIVER TRIFID': 'RETDF TRIFID'},
                    inplace = True)
            RCRAINFO = pd.concat([RCRAINFO, r], ignore_index = True, axis = 0)
            del r
        RCRA_facility = pd.merge(RCRA_facility, RCRAINFO, how = 'left',
                                left_on = ['SRS CHEMICAL ID', 'RECEIVER TRIFID', 'RCRA Waste Management'],
                                right_on = ['SRS CHEMICAL ID', 'RETDF TRIDID', 'RCRA Waste Management']) \
                          .drop_duplicates(keep = 'first')
        del RCRAINFO, Files
        RCRA_facility.rename(columns = {'Type of waste management': 'TYPE OF WASTE MANAGEMENT',
                                    'General': 'GENERAL WASTE MANAGEMENT'},
                            inplace = True)
        RCRA_facility.drop(['TRI Waste Management', 'RCRA Waste Management'],
                        axis = 1, inplace = True)
        No_successful = RCRA_facility.loc[pd.isnull(RCRA_facility['RETDF TRIDID'])]
        RCRA_facility = RCRA_facility.loc[pd.notnull(RCRA_facility['RETDF TRIFID'])]
        RCRA_facility['COHERENCY'] = 'F'
        RCRA_facility.loc[RCRA_facility['QUANTITY RECEIVED'] \
                        >= RCRA_facility['QUANTITY TRANSFER OFF-SITE'], 'COHERENCY'] = 'T' # Flow coherency
        grouping = columns_grouping + ['TYPE OF WASTE MANAGEMENT','GENERAL WASTE MANAGEMENT' , \
                  'RETDF TRIFID', 'RETDF FRS ID']
        RCRA_facility = RCRA_facility.join(RCRA_facility.groupby(grouping)['COHERENCY'].apply(lambda x: 'F' if (x == 'F').all() else 'T'), on = grouping, rsuffix = '_r')
        No_coherent = RCRA_facility.loc[RCRA_facility['COHERENCY_r'] == 'F'].drop_duplicates(keep = 'first', subset = grouping)
        No_successful = pd.concat([No_successful, No_coherent], ignore_index = True, axis = 0)
        del No_coherent, columns_grouping
        No_successful.drop(columns = ['GENERAL WASTE MANAGEMENT', 'TYPE OF WASTE MANAGEMENT',
                                    'RECEIVING YEAR', 'QUANTITY RECEIVED',
                                    'RETDF FRS ID', 'RETDF TRIFID',
                                    'COHERENCY', 'COHERENCY_r'],
                        inplace = True)
        RCRA_facility = RCRA_facility.loc[RCRA_facility['COHERENCY'] == 'T']
        RCRA_facility['Year_difference'] = RCRA_facility.apply(lambda row:
                abs(row['RECEIVING YEAR'] - row['REPORTING YEAR']), \
                 axis = 1)
        RCRA_facility = RCRA_facility.loc[RCRA_facility.groupby(grouping, as_index = False)\
                                     .Year_difference.idxmin()] # Selecting between years
        del grouping
        RCRA_facility.drop(['Year_difference', 'RECEIVING YEAR', 'COHERENCY', 'COHERENCY_r', 'QUANTITY RECEIVED'], axis = 1, inplace = True)
        TRI_facility = pd.concat([TRI_facility, No_successful], ignore_index = True, axis = 0)
        del No_successful
        # Searching information for TRI facilities
        TRI_facility['RETDF FRS ID'] = TRI_facility['RECEIVER FRS ID']
        TRI_facility['RETDF TRIFID'] = TRI_facility['RECEIVER TRIFID']
        TRI_facility = pd.merge(TRI_facility, Management, how = 'left', left_on = 'OFF-SITE WASTE MANAGEMENT', \
                            right_on = 'TRI Waste Management')
        TRI_facility.rename(columns = {'Type of waste management': 'TYPE OF WASTE MANAGEMENT',
                                    'General': 'GENERAL WASTE MANAGEMENT'},
                            inplace = True)
        TRI_facility.drop(['TRI Waste Management', 'RCRA Waste Management'],
                        axis = 1, inplace = True)
        TRI_facility.drop_duplicates(keep = 'first', inplace = True)
        No_broker = pd.concat([TRI_facility, RCRA_facility], ignore_index = True, axis = 0)
        del TRI_facility, RCRA_facility
        # Calling releases
        Path_TRI = self._dir_path + '/../Waste_Tracking/CSV/On_site_tracking/'
        Files_r = [File for File in os.listdir(Path_TRI) if File.startswith('TRI')]
        df_TRI = pd.DataFrame()
        for File_r in Files_r:
            Releases = pd.read_csv(Path_TRI + File_r)
            df_TRI = pd.concat([Releases, df_TRI], ignore_index = True, axis = 0)
            del Releases
        del Files_r
        df_TRI.drop(columns = ['UNIT OF MEASURE'], inplace = True)
        df_TRI['Year_difference'] = df_TRI.apply(lambda row:
                abs(int(self.year) - row['REPORTING YEAR']), \
                 axis = 1)
        grouping = ['TRIFID', 'CAS NUMBER', 'PRIMARY NAICS CODE', 'COMPARTMENT', \
                    'NAICS Title']
        df_TRI = df_TRI.loc[df_TRI.groupby(grouping, as_index = False)\
                                              .Year_difference.idxmin()] # Selecting between years
        del grouping
        df_TRI['TEMPORAL CORRELATION OF RETDF'] = df_TRI.Year_difference.apply(lambda x: self._temporal_correlation(x))
        df_TRI.drop(['Year_difference'], axis = 1, inplace = True)
        df_TRI.rename(columns = {'REPORTING YEAR': 'RETDF REPORTING YEAR'}, inplace = True)
        Facility = pd.read_csv(Path_TRI + 'Facility_Information.csv')
        df_TRI = pd.merge(df_TRI, Facility, how = 'inner', on = 'TRIFID')
        df_TRI.drop_duplicates(keep = 'first', inplace = True)
        df_TRI.rename(columns = {'NAICS Title': 'RETDF PRIMARY NAICS TITLE',
                                'PRIMARY NAICS CODE': 'RETDF PRIMARY NAICS CODE',
                                'TOTAL WASTE': 'TOTAL WASTE GENERATED',
                                'TOTAL WASTE RELIABILITY': 'TOTAL WASTE GENERATED RELIABILITY',
                                'MAXIMUM AMOUNT ON-SITE': 'RETDF MAXIMUM AMOUNT ON-SITE',
                                'CAS NUMBER': 'TRI CHEMICAL ID NUMBER',
                                'TRIFID': 'RETDF TRIFID',
                                'FACILITY NAME': 'RETDF NAME',
                                'FACILITY STREET': 'RETDF STREET',
                                'FACILITY CITY': 'RETDF CITY',
                                'FACILITY COUNTY': 'RETDF COUNTY',
                                'FACILITY STATE': 'RETDF STATE',
                                'FACILITY ZIP CODE': 'RETDF ZIP'},
                    inplace = True)
        No_broker = pd.merge(No_broker, df_TRI, how = 'inner',
                            on = ['TRI CHEMICAL ID NUMBER', \
                                  'RETDF TRIFID'])
        No_broker.drop_duplicates(keep = 'first', inplace = True)
        No_broker['MAXIMUM POSSIBLE FLOW'] = No_broker.apply(lambda row: \
                                            min([row['TOTAL RELEASE'],
                                                row['QUANTITY TRANSFER OFF-SITE']]),
                                            axis = 1)
        No_broker['MAXIMUM POSSIBLE FLOW RELIABILITY'] = No_broker.apply(lambda row: \
                                            max([row['TOTAL RELEASE RELIABILITY'],
                                                row['RELIABILITY OF OFF-SITE TRANSFER']]),
                                            axis = 1)
        No_broker['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW'] = No_broker['TEMPORAL CORRELATION OF RETDF']
        No_broker = No_broker.where((pd.notnull(No_broker)), None)
        No_broker = self._Normalizing_NAICS(No_broker)
        cols = cols[0:28] +  ['GENERAL WASTE MANAGEMENT', 'TYPE OF WASTE MANAGEMENT'] + \
            cols[28:] + ['RETDF FRS ID', 'RETDF TRIFID', \
            'RETDF NAME', 'RETDF STREET', 'RETDF CITY', 'RETDF COUNTY', \
            'RETDF STATE', 'RETDF ZIP', 'RETDF PRIMARY NAICS CODE', \
            'RETDF PRIMARY NAICS TITLE', 'RETDF REPORTING YEAR', 'TEMPORAL CORRELATION OF RETDF', \
            'RETDF MAXIMUM AMOUNT ON-SITE', 'TOTAL WASTE GENERATED', 'TOTAL WASTE GENERATED RELIABILITY', \
            'MAXIMUM POSSIBLE FLOW', 'MAXIMUM POSSIBLE FLOW RELIABILITY', 'TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW', \
            'COMPARTMENT', 'FLOW TO COMPARTMENT', 'FLOW TO COMPARTMENT RELIABILITY', 'TOTAL RELEASE', 'TOTAL RELEASE RELIABILITY']
        No_broker = No_broker[cols]
        No_broker.to_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_off_site_tracking_and_compartments_' + self.year + '_EoL.csv',
                                   sep = ',', index = False)
        del No_broker
        #------------------------------- Brokers -------------------------------#
        Broker = Broker.where((pd.notnull(Broker)), None)
        Path = self._dir_path + '/../Waste_Tracking/CSV/Off_site_tracking/'
        Track = pd.DataFrame()
        # RCRAInfo files
        Files = [File for File in os.listdir(Path) if (File.startswith('RCRAInfo'))]
        for File in Files:
            df = pd.read_csv(Path + File, usecols = ['REPORTING YEAR', 'GENERATOR FRS ID',
                                                    'SRS INTERNAL TRACKING NUMBER', 'QUANTITY RECEIVED',
                                                    'QUANTITY TRANSFERRED', 'RELIABILITY', 'FOR WHAT IS TRANSFERRED',
                                                    'RECEIVER FRS ID', 'RECEIVER TRIFID'])
            Track = pd.concat([Track, df], ignore_index = True, axis = 0)
        del df
        # TRI files
        Files = [File for File in os.listdir(Path) if File.startswith('TRI')]
        WM_match = Management[['TRI Waste Management', 'RCRA Waste Management']]
        WM_match.drop_duplicates(keep = 'first', inplace = True, subset = 'TRI Waste Management')
        WM_match.loc[WM_match['TRI Waste Management'].str.contains('broker',  na = False), 'RCRA Waste Management'] = \
                    'Storage and Transfer -The site receiving this waste stored/bulked and transferred the waste with no reclamation, recovery, destruction, treatment, or disposal at that site'
        for File in Files:
            df = pd.read_csv(Path + File, usecols = ['REPORTING YEAR', 'GENERATOR FRS ID',
                                                    'SRS INTERNAL TRACKING NUMBER',
                                                    'QUANTITY TRANSFERRED', 'RELIABILITY', 'FOR WHAT IS TRANSFERRED',
                                                    'RECEIVER FRS ID', 'RECEIVER TRIFID'])
            df = pd.merge(df, WM_match, how = 'left', left_on = 'FOR WHAT IS TRANSFERRED', \
                        right_on = 'TRI Waste Management').drop_duplicates(keep = 'first')
            df['FOR WHAT IS TRANSFERRED'] = df['RCRA Waste Management']
            df.drop(columns = ['TRI Waste Management', 'RCRA Waste Management'], inplace = True)
            Track = pd.concat([Track, df], ignore_index = True, axis = 0)
        del df, WM_match, Files
        Track.drop_duplicates(keep = 'first', inplace = True)
        Path_saved = dict()
        for index, row in Broker.iterrows():
            aux_tuple = tuple(row[['OFF-SITE WASTE MANAGEMENT',
                                        'RECEIVER FRS ID',
                                        'SRS CHEMICAL ID',
                                        'RCRAInfo CHEMICAL ID NUMBER']])
            if not aux_tuple in Path_saved.keys():
                Paths = self._Off_tracker(row['OFF-SITE WASTE MANAGEMENT'],
                                            row['RECEIVER FRS ID'],
                                            row['SRS CHEMICAL ID'],
                                            row['RCRAInfo CHEMICAL ID NUMBER'],
                                            Track,
                                            Management)
            else:
                Paths = Path_saved[aux_tuple]
            if not Paths.empty:
                if not aux_tuple in Path_saved.keys():
                    Paths_aux = Paths.copy()
                    Path_saved.update({aux_tuple: Paths_aux})
                    del Paths_aux
                df = pd.merge(row.to_frame().T, Paths, how = 'inner',
                            on = ['OFF-SITE WASTE MANAGEMENT',
                                'RECEIVER FRS ID',
                                'SRS CHEMICAL ID',
                                'RCRAInfo CHEMICAL ID NUMBER'])
                del Paths
                df = pd.merge(df, df_TRI, how = 'inner',
                                    on = ['TRI CHEMICAL ID NUMBER',
                                          'RETDF TRIFID'])
                df.drop_duplicates(keep = 'first', inplace = True)
                if not df.empty:
                    df['MAXIMUM POSSIBLE FLOW'] = \
                                df.apply(lambda x: min([x['MAXIMUM POSSIBLE FLOW'],
                                                        x['TOTAL RELEASE'],
                                                        x['QUANTITY TRANSFER OFF-SITE']]),
                                                        axis =  1)
                    df['MAXIMUM POSSIBLE FLOW RELIABILITY'] = \
                                df.apply(lambda x: max([x['MAXIMUM POSSIBLE FLOW RELIABILITY'],
                                                        x['TOTAL RELEASE RELIABILITY'],
                                                        x['RELIABILITY OF OFF-SITE TRANSFER']]),
                                                        axis =  1)
                    df['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW'] = \
                                df.apply(lambda x: max([x['TEMPORAL CORRELATION OF MAXIMUM POSSIBLE FLOW'],
                                                        x['TEMPORAL CORRELATION OF RETDF']]),
                                                        axis =  1)
                    df = self._Normalizing_NAICS(df)
                    df = df[cols]
                    df.to_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_off_site_tracking_and_compartments_' + self.year + '_EoL.csv',
                            sep = ',', index = False, header = False, mode = 'a')
                    del df
                else:
                    continue
            else:
                Paths_aux = Paths.copy()
                Path_saved.update({aux_tuple: Paths_aux})
                del Paths_aux, Paths
        del Track, Management, df_TRI


    def comptox_tsca_groups(self):
        # Calling information from CompTox
        Path_AIM = self._dir_path + '/../../ancillary/others/TRI_CompTox_AIM.csv'
        TSCA_Groups = pd.read_csv(Path_AIM, header = 0, sep = ',', \
                     usecols = ['SMILES', 'CHEMICAL CATEGORY 1', 'CHEMICAL CATEGORY 2', 'CHEMICAL CATEGORY 3', 'ID'])
        # Calling database
        TRI = pd.read_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_off_site_tracking_and_compartments_' + self.year + '_EoL.csv',
                                 header = 0, sep = ',', low_memory = False,
                                 converters = {'TRI CHEMICAL ID NUMBER': lambda x: self._transform_CAS(x)})
        cols = list(TRI.columns)
        TRI = pd.merge(TRI, TSCA_Groups, how = 'left',
                         left_on = 'TRI CHEMICAL ID NUMBER',
                         right_on = 'ID') \
                         .drop_duplicates(keep = 'first')
        cols = cols[0:15] + ['SMILES', 'CHEMICAL CATEGORY 1', 'CHEMICAL CATEGORY 2', 'CHEMICAL CATEGORY 3', 'ID'] \
             + cols[15:]
        TRI = TRI[cols]
        TRI.drop('ID', axis = 1, inplace = True)
        TRI.to_csv(self._dir_path + '/' + self.year + '/TRI_SRS_FRS_off_site_tracking_and_compartments_CompTox_' + self.year + '_EoL.csv',
                                 sep = ',', index = False)


    def _searching_equivalent_naics(self, df, naics):
        year = df['RETDF REPORTING YEAR'].iloc[0]
        if year >= 2017:
            return df
        else:
            if (year < 2017) and (year >= 2012):
                df = pd.merge(df, naics[['2012 NAICS Code', '2017 NAICS Code', '2017 NAICS Title']],
                            how = 'left', left_on = 'RETDF PRIMARY NAICS CODE',
                            right_on = '2012 NAICS Code')
                df.drop(columns = '2012 NAICS Code', inplace = True)
            elif (year < 2012) and (year >= 2007):
                df = pd.merge(df, naics[['2007 NAICS Code', '2017 NAICS Code', '2017 NAICS Title']],
                            how = 'left', left_on = 'RETDF PRIMARY NAICS CODE',
                            right_on = '2007 NAICS Code')
                df.drop(columns = '2007 NAICS Code', inplace = True)
            elif (year < 2007) and (year >= 2002):
                df = pd.merge(df, naics[['2002 NAICS Code', '2017 NAICS Code', '2017 NAICS Title']],
                            how = 'left', left_on = 'RETDF PRIMARY NAICS CODE',
                            right_on = '2002 NAICS Code')
                df.drop(columns = '2002 NAICS Code', inplace = True)
            elif (year < 2002) and (year >= 1997):
                df = pd.merge(df, naics[['1997 NAICS Code', '2017 NAICS Code', '2017 NAICS Title']],
                            how = 'left', left_on = 'RETDF PRIMARY NAICS CODE',
                            right_on = '1997 NAICS Code')
                df.drop(columns = '1997 NAICS Code', inplace = True)
            df.drop_duplicates(keep = 'first', inplace = True)
            idx = df.loc[pd.notnull(df['2017 NAICS Title'])].index.tolist()
            df['RETDF PRIMARY NAICS CODE'].loc[idx] = df['2017 NAICS Code'].loc[idx]
            df['RETDF PRIMARY NAICS TITLE'].loc[idx] = df['2017 NAICS Title'].loc[idx]
            df.drop(columns = ['2017 NAICS Code', '2017 NAICS Title'], inplace = True)
            return df


    def _Normalizing_NAICS(self, TRI):
        # Calling NAICS changes
        Path_naics = self._dir_path + '/../../ancillary/Others'
        naics_files = [file for file in os.listdir(Path_naics) if re.search(r'\d{4}_to_\d{4}_NAICS.csv', file)]
        naics_files.sort()
        for i, file in enumerate(naics_files):
            df_aux = pd.read_csv(Path_naics + '/' + file, low_memory = False,
                                sep = ',', header = 0)
            if i != 0:
                cols = list(df_aux.iloc[:, 0:2].columns)
                df = pd.merge(df, df_aux, how = 'outer', on = cols)
                df.drop_duplicates(keep = 'first', inplace = True)
            else:
                df = df_aux
        TRI.sort_values(by = ['RETDF REPORTING YEAR'], inplace = True)
        TRI.reset_index(inplace = True)
        TRI = TRI.groupby('RETDF REPORTING YEAR', as_index = False).apply(lambda x: self._searching_equivalent_naics(x, df))
        TRI['RETDF PRIMARY NAICS CODE'] = TRI['RETDF PRIMARY NAICS CODE'].astype(pd.Int32Dtype())
        return TRI


if __name__ == '__main__':

    parser = argparse.ArgumentParser(argument_default = argparse.SUPPRESS)

    parser.add_argument('Option',
                        help = 'What do you want to do:\
                        [A]: Organize files.\
                        [B]: Retrieve Chemical information from SRS. \
                        [C]: Add FRS infomation.\
                        [D]: Search Flows\
                        [E]: Assigning TSCA groups', \
                        type = str)

    parser.add_argument('Year',
                        help = 'What TRI year do you want to organize?.',
                        type = str)

    parser.add_argument('-F', '--Files', nargs = '+',
                        help = 'What TRI Files do you want (e.g., 1a, 2a, etc).\
                        Check:\
                        https://www.epa.gov/toxics-release-inventory-tri-program/tri-basic-plus-data-files-guides',
                        required = False,
                        default = None)


    args = parser.parse_args()

    TRIyear = args.Year
    TRIfiles = args.Files
    start_time =  time.time()

    if args.Option == 'A':
        TRI = TRI_EoL(TRIyear, Files = TRIfiles)
        TRI.generate_dataframe()
    elif args.Option == 'B':
        TRI = TRI_EoL(TRIyear)
        TRI.srs_search()
    elif args.Option == 'C':
        TRI = TRI_EoL(TRIyear)
        TRI.frs_search()
    elif args.Option == 'D':
        TRI = TRI_EoL(TRIyear)
        TRI.flows_search()
    elif args.Option == 'E':
        TRI = TRI_EoL(TRIyear)
        TRI.comptox_tsca_groups()

    print('Execution time: %s sec' % (time.time() - start_time))
