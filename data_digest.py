
# coding: utf-8

# In[9]:

import pandas as pd
import numpy as np
import os

from sqlalchemy import *
from datetime import datetime

# File types
renew_datatypes = {'Plant Name': str,
                  'Unit Name': str ,
                  'ORISPL (Plant Code)': str,
                   'GATS Unit ID': str,
                   'PJM Unit?': str,
                   'State': str,
                   'County': str,
                   'Balancing Authority': str,
                   'Nameplate': np.float64,
                   'Date Online': str,
                   'Primary Fuel Type': str,
                   'Secondary': str,
                   'Tertiary': str,
                   'Quaternary': str,
                   'Fifth': str,
                   'Sixth': str,
                   'Seventh' : str,
                   'Eighth': str,
                   'New Jersey': str,
                   'Maryland': str,
                   'Pennsylvania': str,
                   'District of Columbia': str,
                   'Delaware': str,
                   'Illinois': str,
                   'Ohio': str,
                   'Virginia': str,
                   'Green-e Eligible': str,
                   'EFEC Eligible': str}

renew_expected_cols = ['Plant Name',
 'Unit Name',
 'ORISPL (Plant Code)',
 'GATS Unit ID',
 'PJM Unit?',
 'State',
 'County',
 'Balancing Authority',
 'Nameplate',
 'Date Online',
 'Primary Fuel Type',
 'Secondary',
 'Tertiary',
 'Quaternary',
 'Fifth',
 'Sixth',
 'Seventh',
 'Eighth',
 'New Jersey',
 'Maryland',
 'Pennsylvania',
 'District of Columbia',
 'Delaware',
 'Illinois',
 'Ohio',
 'Virginia',
 'Green-e Eligible',
 'EFEC Eligible']

renew_new_cols = ['plant_name',
    'unit_name',
    'plant_code',
    'gats_id',
    'pjm_unit',
    'state',
    'county',
    'balancing_authority',
    'nameplate',
    'date_online',
    'primary_fuel',
    'secondary',
    'tertiary',
    'quaternary',
    'fifth',
    'sixth',
    'seventh',
    'eighth',
    'new_jersey_id',
    'maryland_id',
    'pennsylvania_id',
    'district_of_columbia_id',
    'delaware_id',
    'illinois_id',
    'ohio_id',
    'virginia_id',
    'green-e',
    'efec_eligible']

renew_cols_to_drop = ['efec_eligible', 'green-e']


gen_datatypes = {'Unit ID': str,
                  'Plant Name': str ,
                  'Unit Name': str,
                   'Balancing Authority': str,
                   'Fuel Type': str,
                   'Owner Type': str,
                   'New Jersey': str,
                   'Maryland': str,
                   'District of Columbia': str,
                   'Pennsylvania': str,
                   'Delaware': str,
                   'Illinois': str,
                   'Ohio': str,
                   'West Virginia': str,
                   'Virginia': str,
                   'Green-e Energy Eligible': str,
                   'EFEC Eligible': str}

gen_expected_cols = ['Unit ID',
 'Plant Name',
 'Unit Name',
 'Balancing Authority',
 'Fuel Type',
 'Owner Type',
 'New Jersey',
 'Maryland',
 'District of Columbia',
 'Pennsylvania',
 'Delaware',
 'Illinois',
 'Ohio',
 'West Virginia',
 'Virginia',
 'Green-e Energy Eligible',
 'EFEC Eligible']

gen_new_cols = ['unit_id',
    'plant_name',
    'unit_name',
    'balancing_authority',
    'fuel_type',
    'owner_type',
    'new_jersey_tier',
    'maryland_tier',
    'district_of_columbia_tier',
    'pennsylvania_tier',
    'delaware_tier',
    'illinois_tier',
    'ohio_tier',
    'west_virginia_tier',
    'virginia_tier',
    'green_e',
    'efec_eligible']


gen_cols_to_drop = ['plant_name',
    'unit_name',
    'balancing_authority',
    'efec_eligible']



# In[10]:

def digest(renew_file, gen_file):

    renew_frame = pd.read_csv(renew_file, dtype=renew_datatypes)
    gen_frame = pd.read_csv(gen_file, dtype=gen_datatypes)

    assert list(renew_frame.columns) == renew_expected_cols, "Alert: PJM-GATS has changed it's schema for 'renew'."
    assert list(gen_frame.columns) == gen_expected_cols, "Alert: PJM-GATS has changed it's schema for 'gen'."
    
    # Data clean, column rename and dumping
    renew_frame['Date Online'] = renew_frame['Date Online'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y'))
    renew_frame.columns = renew_new_cols
    gen_frame.columns = gen_new_cols
    renew_frame.drop(renew_cols_to_drop, axis=1, inplace=True)
    gen_frame.drop(gen_cols_to_drop, axis=1, inplace=True)

    # Fix ids
    renew_frame['gats_id'] = renew_frame['gats_id'].apply(lambda x: x.strip('NONMSET'))
    gen_frame['unit_id'] = gen_frame['unit_id'].apply(lambda x: x.strip('NONMSET'))

    # Key merge
    all_frame = pd.merge(renew_frame, gen_frame, left_on='gats_id', right_on='unit_id',how='inner')
    all_frame.drop('unit_id', axis=1, inplace=True)

    return all_frame


def database_dump(all_frame, location):

    # Get data 
    engine = create_engine(location)   

    # Name table
    day = str(datetime.now().date().day)
    month = str(datetime.now().date().month)
    year = str(datetime.now().date().year)
    table_name = 'units_'+month+'_'+day+'_'+year

    # Create new table
    metadata = MetaData()
    new_table = Table(table_name, metadata,
        Column('plant_name', String(80)),
        Column('unit_name', String(80)),
        Column('plant_code', String(80)),
        Column('gats_id', String(80), primary_key=True),              
        Column('pjm_unit', String(80)),
        Column('state', String(80)),
        Column('county', String(80)),
        Column('balancing_authority', String(80)),
        Column('nameplate', Float),
        Column('date_online', Date),
        Column('primary_fuel', String(80)),
        Column('secondary', String(80)),
        Column('tertiary', String(80)),
        Column('quaternary', String(80)),
        Column('fifth', String(80)),
        Column('sixth', String(80)),
        Column('seventh', String(80)),
        Column('eighth', String(80)),
        Column('new_jersey_id', String(80)),
        Column('maryland_id', String(80)),
        Column('pennsylvania_id', String(80)),
        Column('district_of_columbia_id', String(80)),
        Column('delaware_id', String(80)),
        Column('illinois_id', String(80)),
        Column('ohio_id', String(80)),
        Column('virginia_id', String(80)),
        Column('fuel_type', String(80)),
        Column('owner_type', String(80)),
        Column('new_jersey_tier', String(80)),
        Column('maryland_tier', String(80)),
        Column('district_of_columbia_tier', String(80)),
        Column('pennsylvania_tier', String(80)),
        Column('delaware_tier', String(80)),
        Column('illinois_tier', String(80)),
        Column('ohio_tier', String(80)),
        Column('west_virginia_tier', String(80)),
        Column('virginia_tier', String(80)),
        Column('green_e', String(80)))

    new_table.create(engine)

    # Insert data
    all_frame.to_sql(table_name,engine,if_exists='replace',index=False)

    # Return the frame
    return all_frame




# In[ ]:



