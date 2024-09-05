import pandas as pd
import re
import datetime as dt


df = pd.read_csv('/Users/karanmanoharan/Task/monster_com-job_sample.csv')
df.describe()
df.info()


df.isnull().sum()  # date added and salary have the most null values



def find_missing_values_in_columns(df, columns):
    missing_percentage = df[columns].isnull().sum() * 100 / len(df)
    return missing_percentage


columns_to_check = ["date_added", "salary", "sector", "job_type", "organization"]

find_missing_values_in_columns(df, columns_to_check)

# We can conclude 5 columns have missing values which are date_added, salary, sector, job_type, organization



df['has_expired'].unique()
# All the jobs are new , none of them expired.[NEW JOBS ONLY WITH NO EXPIRY]
df.sample(15)
df['job_board'].unique()
df['country'].unique()


df.nunique()
# DROPPING country, country code, job board and has_expired as they have only 1 unique value and cannot generate much insight


df['job_type'].unique()

def clean_job_type(job_type):
    """
    Cleans the job_type column by normalizing the text and mapping the values to standard categories.
    """
    if pd.isna(job_type):
        return "Unknown"

    # Normalize text: lowercase and remove special characters
    job_type = job_type.lower()
    job_type = re.sub(r'\s+', ' ', job_type)  # Remove extra spaces
    job_type = job_type.replace('\xa0', ' ').strip()  # Remove non-breaking spaces

    # Define standard job types and map the existing values to them
    if 'full time' in job_type:
        if 'temporary' in job_type or 'contract' in job_type or 'project' in job_type:
            return 'Full Time Contract'
        else:
            return 'Full Time'
    elif 'part time' in job_type:
        if 'temporary' in job_type or 'contract' in job_type or 'project' in job_type:
            return 'Part Time Contract'
        else:
            return 'Part Time'
    elif 'per diem' in job_type:
        return 'Per Diem'
    elif 'temporary' in job_type or 'contract' in job_type:
        return 'Contract'
    elif 'intern' in job_type:
        return 'Intern'
    elif 'exempt' in job_type:
        return 'Exempt'
    else:
        return 'Other'


df['cleaned_job_type'] = df['job_type'].apply(clean_job_type)


df['cleaned_job_type'].unique()
# Reduced 39 unique values into 9 by cleaning them

df.info()


df.sample(25)


df['location'][2]


df['organization'][2]


loc = df.location.unique()
unique_location = {}
for i in range(len(loc)):
    unique_location[i] = loc[i]

for k in df.organization.keys():
    if df.organization[k] in loc:
        temp = df.organization[k]
        df.organization[k] = df.location[k]
        df.location[k] = temp

# in some columns after sampling I discovered that the location and organization had values swapped values by sampling


# Since location has varying values I limit it to 25 characters being max
df = df[df['location'].apply(lambda x: len(x) < 25)]
location = df['location'].str.split(',')
df['location'] = location.str[0]


df['location'].nunique()


# Removing zip code from location
pattern = r'[A-Z/a-z]'
df = df[df['location'].str.contains(pattern)]


df['location']


df['job_title'] = df['job_title'].astype(str)


df['job_title'].nunique()


df['job_title'] = df['job_title'].str.lower()


df['job_title']


df['job_title'].nunique()
# We have found 38 duplicate job titles


df.info()


df.dtypes



def remove_special_characters(text):
    return re.sub(r'[^A-Za-z0-9\s]', '', text)


df['job_title'] = df['job_title'].apply(remove_special_characters)


df['job_title']


df['salary'].nunique()


import numpy as np



def min_max_hourly_wage(val):
    if pd.isnull(val):
        return np.nan
    elif ' /hour' in val:
        i = val.split('/hour')[0].replace('$', ' ').strip()
        if '-' in i:
            mn = i.split('-')[0]
            mx = i.split('-')[1]
            # Since there are several commas in salary. It will take it as a string. So it will throw an
            # error saying "unsupported operand type(s) for /: 'str' and 'int'" and will not be able to
            # perform operation. And, I used try catch block because there is € currency in salary also. So in order
            # to deal with this I preferred try catch block.
            try:
                mn = float(mn.replace(",", "").strip())
                mx = float(mx.replace(",", "").strip())
            except:
                return np.nan
            return mn, mx


def min_max_yearly_wage(val):
    if pd.isnull(val):
        return np.nan
    elif ' /year' in val:
        i = val.split('/year')[0].replace('$', ' ').strip()
        if '-' in i:
            mn = i.split('-')[0]
            mx = i.split('-')[1]
            # Since there are several commas in salary. It will take it as a string. So it will throw an
            # error saying "unsupported operand type(s) for /: 'str' and 'int'" and will not be able to
            # perform operation. And, I used try catch block because there is € currency in salary also. So in order
            # to deal with this I preferred try catch block.
            try:
                mn = float(mn.replace(",", "").strip())
                mx = float(mx.replace(",", "").strip())
            except:
                return np.nan
            return mn, mx


df = df.assign(yearly_salary_range=df['salary'].apply(min_max_yearly_wage),
               hourly_salary_range=df['salary'].apply(min_max_hourly_wage))
df = df.assign(
    median_yearly_salary=df['yearly_salary_range'].apply(
        lambda r: (r[0] + r[1]) / 2 if pd.notnull(r) else r
    ),
    median_hourly_salary=df['hourly_salary_range'].apply(
        lambda r: (r[0] + r[1]) / 2 if pd.notnull(r) else r
    )
)


df.drop(columns=['salary'], inplace=True)
df
df.to_csv('silver.csv')


df[['median_yearly_salary', 'median_hourly_salary', 'yearly_salary_range', 'hourly_salary_range']] = df[
    ['median_yearly_salary', 'median_hourly_salary', 'yearly_salary_range', 'hourly_salary_range']].fillna('NA')


df['sector'].unique()  # Already cleaned and organized



# DROPPING COLUMNS WHICH IS NOT IMPORTANT
# """The country column and country code is already mentioned,
# the date added is of no importance as we take our own ingestion date,
# has_expired has only 1 unique value and is the same
# job_board is the same for all"""


df



df_gold_dataset = df[['job_title', 'job_description', 'page_url', 'location', 'cleaned_job_type']]
df_gold_dataset.isna().sum()
df_gold_dataset.to_csv('gold.csv', index=False)
df["ingestion_timestamp"] = dt.datetime.now()





