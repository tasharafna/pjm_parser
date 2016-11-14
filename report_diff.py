
# coding: utf-8

# In[39]:

import pandas as pd

from sqlalchemy import Column, ForeignKey, Integer, String, Float, Date, Boolean, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import numpy as np
import os

from datetime import datetime, timedelta

import yagmail

import xlsxwriter


# Below which we don't care about changes - current set at 1 MW
THRESHOLD = 1

# List of columns for which we care about changes
columns_to_check_duplication = ['plant_name',
    'unit_name',
    'gats_id',
    'balancing_authority',
    'primary_fuel',
    'secondary',
    'new_jersey_id',
    'maryland_id',
    'pennsylvania_id',
    'district_of_columbia_id',
    'delaware_id',
    'illinois_id',
    'ohio_id',
    'fuel_type',
    'owner_type',
    'new_jersey_tier',
    'maryland_tier',
    'pennsylvania_tier',
    'district_of_columbia_tier',
    'delaware_tier',
    'illinois_tier',
    'ohio_tier']


# In[47]:

# Helper function for reporting differences
def report_diff(x):
    if pd.isnull(x[0]) and pd.isnull(x[1]):
        return x[0]
    
    if x[0] == x[1]:
        return x[0]
    
    elif isinstance(x[0], float):
        return x[0]
    # This is a necessary kludge since we're having problems comparing nameplate
    # Our function thinks many plants have changed capacity by 0.00001 MW
    
    else:
        changed_line = '{} ---> {}'.format(*x)
        return changed_line


    
# Main function for parsing two units pandas frames 
def get_units_differences(old, new):
    
    print "diff function begin"
    # Define
    units_old = old
    units_new = new
    
    # Add field
    units_old['version'] = 'old'
    units_new['version'] = 'new'

    # Combine
    double_frame = pd.concat([units_old, units_new], ignore_index=True)

    # Drop duplicates
    unique_frame = double_frame.drop_duplicates(subset = columns_to_check_duplication)

    # Code to identify and parse changed rows
    id_repeats = unique_frame.set_index('gats_id').index.get_duplicates()
    changes = unique_frame[unique_frame['gats_id'].isin(id_repeats)]
    
    # Test
    assert changes.shape[0] < 2000, 'Greater than 2000 changed plants, so something is likely afoot.'

    # Get old and new versions of all changed rows
    changes_new = changes[changes['version'] == 'new']
    changes_old = changes[changes['version'] == 'old']
    changes_new = changes_new.drop(['version'], axis=1)
    changes_old = changes_old.drop(['version'], axis=1)
    new_indexed = changes_new.set_index('gats_id')
    old_indexed = changes_old.set_index('gats_id')
    diff_panel = pd.Panel({'df1': old_indexed,
                          'df2': new_indexed})

    # Use helper function to 'mark' changed cells
    diff_output = diff_panel.apply(report_diff, axis=0)

    print "logger started"
    # Log changed rows
    changed_rows = []
    for field, content in diff_output.iteritems():
        
        if field == 'nameplate':
            continue
            # This is necessary since we're having problems comparing nameplate
            # Our function thinks many plants have changed capacity by 0.00001 MW
        
        i = 0
        for item in content:
            if isinstance(item, str) and '->' in item:
                gats_id = diff_output.index[i]
                nameplate = int(new_indexed['nameplate'][gats_id])
                fuel = new_indexed['primary_fuel'][gats_id]
                plant_name = diff_output['plant_name'][i] # not used
                nameplate_string = str(nameplate)
                
                if nameplate >= THRESHOLD: 
                    try:
                        statement = '%sMW %s changed "%s": %s' % (nameplate_string, fuel, field, item)
                        changed_rows.append(statement)
                    except UnicodeDecodeError:
                        error_statement = '%sMW plant "%s" has an unprintable changed field.' % (nameplate_string, plant_name)
                        print error_statement
            i += 1
      
    # Identify deleted rows
    unique_frame['duplicate'] = unique_frame['gats_id'].isin(id_repeats)
    id_old = units_old['gats_id']
    id_new = units_new['gats_id']
    deleted_frame = units_old[~units_old['gats_id'].isin(id_new)]

    # Log deleted rows
    deleted_rows = []
    for item in deleted_frame.itertuples():
        plant_name = item[1] # not used
        nameplate = item[9]
        state = item[6]
        fuel = item[11]

        if nameplate >= THRESHOLD:
            statement = '%sMW %s plant in %s deleted.' % (str(nameplate), fuel, state)
            deleted_rows.append(statement)
        
    # Log added rows
    added_frame = units_new[~units_new['gats_id'].isin(id_old)]
    added_rows = []
    for item in added_frame.itertuples():
        plant_name = item[1] # not used
        nameplate = item[9]
        state = item[6]
        fuel = item[11]
        
        if nameplate >= THRESHOLD:
            statement = '%sMW %s plant in %s added.' % (str(nameplate), fuel, state)
            added_rows.append(statement)
        
    # Organize frames 
    changed_frame = changes_new
    added_frame = added_frame.drop(['version'], axis=1)
    deleted_frame = deleted_frame.drop(['version'], axis=1)

    # Limit to big plants
    changed_frame = changed_frame[changed_frame['nameplate'] >= THRESHOLD]
    added_frame = added_frame[added_frame['nameplate'] >= THRESHOLD]
    deleted_frame = deleted_frame[deleted_frame['nameplate'] >= THRESHOLD]
    
    # Email changes to me
    email_changes(changed_rows, deleted_rows, added_rows, changed_frame, added_frame, deleted_frame)
                           
    return None




def email_changes(changed_list, deleted_list, added_list, changed_frame, added_frame, deleted_frame):
    
    print 'begin emails'
    to = ['nick.culver@gmail.com','tjacobsen@bluedeltaenergy.com','knelson@bluedeltaenergy.com']


    # Get today's date
    day = str(datetime.now().date().day)
    month = str(datetime.now().date().month)
    year = str(datetime.now().date().year)


    # Initialize
    yag = yagmail.SMTP('bluedeltabot@gmail.com')

    # Send added plants, if any
    subject = 'The following PJM plants were *ADDED* today.'
    contents = added_list
    if len(contents) != 0:

        added_file_name = 'added_'+month+'_'+day+'_'+year+'.xlsx'

        writer_orig = pd.ExcelWriter(added_file_name, engine='xlsxwriter')
        added_frame.to_excel(writer_orig, index=False, sheet_name='units')
        writer_orig.save()

        contents.append(added_file_name)
        yag.send(to, subject, contents)

    # Send deleted plants, if any
    subject = 'The following PJM plants were *DELETED* today.'
    contents = deleted_list
    if len(contents) != 0:

        deleted_file_name = 'deleted_'+month+'_'+day+'_'+year+'.xlsx'

        writer_orig = pd.ExcelWriter(deleted_file_name, engine='xlsxwriter')
        deleted_frame.to_excel(writer_orig, index=False, sheet_name='units')
        writer_orig.save()

        contents.append(deleted_file_name)
        yag.send(to, subject, contents)

    # Send changed plants, if any
    subject = 'The following PJM plants were *CHANGED* today.'
    contents = changed_list
    if len(contents) != 0:

        changed_file_name = 'changed_'+month+'_'+day+'_'+year+'.xlsx'

        writer_orig = pd.ExcelWriter(changed_file_name, engine='xlsxwriter')
        changed_frame.to_excel(writer_orig, index=False, sheet_name='units')
        writer_orig.save()

        contents.append(changed_file_name)
        yag.send(to, subject, contents)

    return None


# In[51]:

def prepare_incumbent_frame(location):

    # create engine - comment out if already done
    engine = create_engine(location)
    
    # Name table - must compare against yesterday's table!
    datetime_today = datetime.now().date()
    datetime_yesterday = datetime_today - timedelta(days=1)
    day = str(datetime_yesterday.day)
    month = str(datetime_yesterday.month)
    year = str(datetime_yesterday.year)
    table_name = 'units_'+month+'_'+day+'_'+year

    # If table does not exists, then find the most recent
    if not engine.dialect.has_table(engine, table_name):
        for i in range(30):

            # Check increasing distant days in past
            recent_datetime = datetime_yesterday - timedelta(days=i)
            day = str(recent_datetime.day)
            month = str(recent_datetime.month)
            year = str(recent_datetime.year)
            table_name = 'units_'+month+'_'+day+'_'+year

            if engine.dialect.has_table(engine, table_name):
                break
        else:
            raise Exception("Can't find incumbent table !!!")


    # Pull entire table into frame
    query = "SELECT * from "+table_name+ ";"
    incumbent_frame = pd.read_sql(query, engine)

    #############
    print table_name, " is incumbent frame."
    ##############
    
    return incumbent_frame




