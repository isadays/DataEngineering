import pandas as pd
import requests
from io import StringIO, BytesIO
import sys
import os
import logging

#following the tutorial: https://docs.python.org/3/howto/logging.html#logging-basic-tutorial

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_ONS_data(year, start_month, end_month, output_folder='ONS_data'):

    os.makedirs(output_folder, exist_ok=True)

    for month in range(start_month, end_month + 1):
        f_month = f"{month:02d}"
        url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_{year}_{f_month}.csv"
        logging.info(f"Processing month: {f_month}...")

        try:
            response = requests.get(url)
            if response.status_code == 200:
                 
                csv_data = pd.read_csv(StringIO(response.text), delimiter=';')

                 
                csv_data = csv_data[['din_instante', 'ceg', 'val_geracao']]
                csv_data.columns = ['din_instante', 'ceg', 'val_geracao']

                 
                csv_data.to_csv(f"{output_folder}/ONS_{year}_{f_month}.csv", index=False)
                logging.info(f"Month {f_month} downloaded .")
            else:
                logging.error(f"Failed to download month {f_month} data. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception month {f_month}: {e}")



def extract_ONS_hist_data(start_year, end_year, output_folder='ONS_data/monthly'):
    os.makedirs(output_folder, exist_ok=True)
    for year in range(start_year, end_year + 1):
        f_year = f"{year:02d}"
        url = f"https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA_20{f_year}.csv"
        logging.info(f"Processing year: {f_year}...")
        
        try:
            response = requests.get(url)
            if response.status_code == 200:
                csv_data = pd.read_csv(StringIO(response.text), delimiter=';')
                
                csv_data['din_instante'] = pd.to_datetime(csv_data['din_instante'])
                
                aggregation_rules = {'val_geracao': 'sum'}
                
                if 'ceg' in csv_data.columns:
                    group_cols = [pd.Grouper(key='din_instante', freq='M'), 'ceg']
                else:
                    group_cols = pd.Grouper(key='din_instante', freq='M')
                
                monthly_data = csv_data.groupby(group_cols).agg(aggregation_rules).reset_index()
                
                monthly_data.to_csv(f"{output_folder}/ONS_{year}.csv", index=False)
                
                logging.info(f"Monthly data for year {year} saved.")
            else:
                logging.error(f"Failed to download year {f_year} data. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for year {f_year}: {e}")

            
def extract_eneva_data(output_folder='Eneva_data', output_file='Eneva_data.csv'):
    #url = 'https://api.mziq.com/mzfilemanager/v2/d/6c663f3b-ae5a-4692-81d3-ab23ee84c1de/ceb34245-e3d4-f4f6-0613-0790f834ad29?origin=1'
    url = 'https://api.mziq.com/mzfilemanager/v2/d/6c663f3b-ae5a-4692-81d3-ab23ee84c1de/8ce5db34-4a35-bbf4-0ba2-25d236cfe922?origin=1'
    os.makedirs(output_folder, exist_ok=True)
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
             
            df = pd.read_excel(BytesIO(response.content), skiprows=4, engine='openpyxl')
            
             
            df['Plant'] = df['Unnamed: 2'].where(df['3Q23'].isna()).ffill()
            
             
            data_df = df[df['3Q23'].notna()].copy()
            
             
            data_df.rename(columns={'Unnamed: 2': 'Variable'}, inplace=True)
            
            
            quarters = df.columns[3:-1]   
            data_df = pd.melt(data_df, id_vars=['Plant', 'Variable'], value_vars=quarters, 
                              var_name='Quarter', value_name='Value')
            
            data_df.to_csv(os.path.join(output_folder, output_file), index=False)
            logging.info(f"Eneva data saved to '{output_file}'.")
        else:
            logging.error(f"Failed to download Eneva data. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command == 'ons':
            if len(sys.argv) == 5:  
                year = int(sys.argv[2])  
                start_month = int(sys.argv[3])
                end_month = int(sys.argv[4])
                extract_ONS_data(year, start_month, end_month)
            else:
                logging.error("Invalid arguments for extract_ONS_data function. Please provide: year start_month end_month.")
        elif command == 'eneva':
            extract_eneva_data()
        elif command == 'historical_ons':
            if len(sys.argv) == 4:
                start_year = int(sys.argv[2])   
                end_year = int(sys.argv[3])
                extract_ONS_hist_data(start_year, end_year)
            else:
                logging.error("Invalid arguments for 'historical_ons' command. Please provide: start_year end_year.")
        elif command == 'both':
            if len(sys.argv) == 5:  
                year = int(sys.argv[2])  
                start_month = int(sys.argv[3])
                end_month = int(sys.argv[4])
                extract_ONS_data(year, start_month, end_month)
                extract_eneva_data()
            else:
                logging.error("Invalid number of arguments for 'both' command. Please provide: year start_month end_month.")
        else:
            logging.error("Invalid command. Use 'ons' followed by year start_month end_month, 'eneva', 'both' followed by year start_month end_month.")
            logging.error("Invalid command. For historical data, use historical ONS followed by start_year end_year")
    else:
        logging.error("No command provided.")
