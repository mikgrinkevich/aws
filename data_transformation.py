import pandas as pd

def process_raw_data():
    # just creating a file with columns that we need
    df = pd.read_csv('data/database.csv',parse_dates=['return'], dtype = {'departure_name': str})
    df = df.rename(columns={'return': 'return_date'})
    df['month'] = df.return_date.dt.strftime('%y_%m')
    df.to_csv('data/database_processed.csv')

def get_only_date_from_df():
    # lightweighted file with only date column to iterate over
    df = pd.read_csv('data/database_processed.csv')
    df = df.filter(['month'])
    df.to_csv('data/date.csv')

def csv_saver():
    df = pd.read_csv('data/date.csv')
    cols = df.columns
    for i in set(df.month):
        filename = "data/"+i+".csv"
        df.loc[df.month == i].to_csv(filename,index=False,columns=cols)



 