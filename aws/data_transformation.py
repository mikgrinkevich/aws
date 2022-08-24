import pandas as pd

def process_raw_data():
    df = pd.read_csv('data/database.csv', parse_dates=['return'], dtype = {'departure_name': str})
    df = df.rename(columns={'return': 'return_date'})
    df['month'] = df.return_date.dt.strftime('%y_%m')
    df.to_csv('data/database_processed.csv')

def get_list_of_months():
    # getting a list to iterate over
    df = pd.read_csv('data/database_processed.csv')
    cols = df.columns
    date_list = list(set(df.month))
    return date_list

def separating_files_by_month_year():
    months_list = get_list_of_months()
    df = pd.read_csv('data/database_processed.csv', parse_dates=['return_date'])
    cols = df.columns
    for i in months_list:
        filename = "data/"+i+".csv"
        df.loc[df.month == i].to_csv(filename,index=False,columns=cols)

def adding_indexes_to_separated_files():
    months_list = get_list_of_months()
    for i in months_list:
        df = pd.read_csv(f'data/{i}.csv')
        df['index'] = df.index
        df.to_csv(f'data/{i}.csv', index=False)

# process_raw_data()
# separating_files_by_month_year()
# adding_indexes_to_separated_files()
