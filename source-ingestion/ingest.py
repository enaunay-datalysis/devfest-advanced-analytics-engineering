import pandas as pd
import argparse
import os
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db

    print('Parsing of args: Done \n')

    url_conn = f'postgresql://{user}:{password}@{host}:{port}/{db}'

    engine = create_engine(url_conn)

    company = 'megaline'
    files = [['calls', 'call_date'], ['internet', 'session_date'], ['messages', 'message_date'], ['plans'], ['users']]

    for file in files:

        data_raw = pd.read_csv(f'./source_data/{company}_{file[0]}.csv')

        print(f'Read {file[0]} raw data: Done')
          
        t_start = time()

        if len(file) == 2:
            data_raw[file[1]] = pd.to_datetime(data_raw[file[1]])

        data_raw.to_sql(name=file[0], con=engine, if_exists='replace', index=False)

        t_end = time()

        print(f'{file[0]} data inserted took %.3f seconds' % (t_end - t_start))
        print()

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Ingesta de datos de CSV a Postgres')

    parser.add_argument('--user', required=True, help='Username para la db en postgres')
    parser.add_argument('--password', required=True, help='Password para la db en postgres')
    parser.add_argument('--host', required=True, help='Host para la db en postgres')
    parser.add_argument('--port', required=True, help='Port para la db en postgres')
    parser.add_argument('--db', required=True, help='DB name para la db en postgres')
    
    args = parser.parse_args()
    
    main(args)