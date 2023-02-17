# -*- coding: utf-8 -*-
import pandas as pd

#grab source files
cons_src = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv'
emails_src = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv'
cons_sub_src = 'https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv'


def sample_src_df(src_url = None, chunksize=100):
    ''' returns the first chunk of a csv file as a dataframe of chunksize = chunksize'''
    if src_url is None:
        src_url = cons_src  
    reader = pd.read_csv(src_url, chunksize=chunksize)
    cons_df = next(reader)
    return cons_df

def read_src_df(src_url = None, chunksize=100, columns=None):
    ''' input: source url of csv file
    chunksize: defaults to 100 records per chunk
    columns: list of columns to be returned
    returns a dataframe of chunksize = chunksize''' 

    reader = pd.read_csv(src_url, chunksize=chunksize)
    #for each chunk, grab the columns cons_id and source and concat them into one dataframe
    df = pd.concat([chunk[columns] for chunk in reader])
    print(f'{len(df)} records read')
    return df

def run():

    #read cons.csv and return relevant columns for processing  using helper function in step 1
    cons_df = read_src_df(cons_src, columns=['cons_id', 'source','create_dt','modified_dt'])

    #replace null values with 'unknown'
    cons_df['source'] = cons_df['source'].fillna('unknown')

    #convert create_dt and modified_dt to datetime for date grouping in later steps
    cons_df['create_dt'] = pd.to_datetime(cons_df['create_dt'], format='%a, %Y-%m-%d %H:%M:%S')
    cons_df['modified_dt'] = pd.to_datetime(cons_df['modified_dt'], format ='%a, %Y-%m-%d %H:%M:%S')

    #column renaming to match desired output 
    cons_df.rename(columns={'modified_dt':'updated_dt'}, inplace=True)
    emails_df = read_src_df(emails_src, columns=['cons_email_id', 'cons_id', 'email','is_primary'])

    #inner join person_df and emails_df on cons_id
    person_email_df = pd.merge(cons_df, emails_df, on='cons_id', how='inner')
    
    #process subscription file
    
    cons_sub_df = read_src_df(cons_sub_src, columns=['cons_email_id', 'chapter_id','isunsub','unsub_dt'])

    #6. Create final merge output of people, email and subscription
    people_raw_df = pd.merge(person_email_df, cons_sub_df, on='cons_email_id', how='inner')

    #only return chapter_id = 1
    people_df = people_raw_df[people_raw_df['chapter_id'] == 1].copy()

    #adding a boolean column that does a group by on cons_id and returns a 1 if both isunsub and is_primary flag is 1. This retains the row granularity.
    people_df['is_unsub'] = people_df.groupby('cons_id')['isunsub'].transform('max') & people_df.groupby('cons_id')['is_primary'].transform('max')
    people_df = people_df[['email', 'source', 'is_unsub','create_dt', 'updated_dt']]

    #export to csv file
    people_df.to_csv('people.csv',index=False)
    
    #create a new dataframe called acquisition df. Since we already converted the columns to datetime, we can utilize the grouper pandas function to roll-up to counts by Day
    acquisition_df = people_df.groupby(pd.Grouper(key='create_dt', freq='D'))['email'].agg(['count']).rename(columns={'count':'acquisitions'}).reset_index()

    #rename create_dt to acquisition_date
    acquisition_df.rename(columns={'create_dt':'acquisition_date'}, inplace=True)

    #save to csv
    acquisition_df.to_csv('acquisitions.csv',index=False)
    print('acquisitions.csv file created')

if __name__ == "__main__":
    print('ETL task completed')