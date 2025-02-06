import requests as r
import json
import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
import numpy as np
import psycopg2
import time
import os
import threading
import subprocess
import logging
import traceback
import smtplib
import gspread
from google.oauth2 import service_account
from openpyxl import load_workbook
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from adsim_config import adsim_token, host, port, dbname, user, password

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="script.log",
    filemode="a",
)

# Initialize report
report = {
    "status": "success",
    "operations": [],
    "errors": [],
}

def log_operation(operation, status, details=None):
    report["operations"].append({
        "operation": operation,
        "status": status,
        "details": details,
    })

def log_error_report(error):
    report["errors"].append({
        "error_type": type(error).__name__,
        "error_message": str(error),
        "traceback": traceback.format_exc(),
    })
    report["status"] = "failed"

def save_report(report):
    """
    Saves the report to a JSON file in a folder named 'reports'.
    If the folder doesn't exist, it creates it.
    """
    # Define the folder name
    reports_folder = Path("reports")
    
    # Create the folder if it doesn't exist
    reports_folder.mkdir(exist_ok=True)
    
    # Generate a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Define the file path
    file_path = reports_folder / f"script_report_{timestamp}.json"
    
    # Save the report to the file
    with open(file_path, "w") as f:
        json.dump(report, f, indent=4)
    
    logging.info(f"Report saved to {file_path}")

end = datetime.today()
end_date = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

start = end - timedelta(minutes=30)
start_date = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

deals_url = f"https://api.adsim.co/crm-r/api/v2/deals?start={start_date}&end={end_date}"
logs_url = f'https://api.adsim.co/crm-r/api/v2/deals/steps/logs?start={start_date}&end={end_date}'
proposals_url = f'https://api.adsim.co/crm-r/api/v2/deals/proposals?start={start_date}&end={end_date}'
organization_url = f"https://api.adsim.co/crm-r/api/v2/entities?start={start_date}&end={end_date}"

headers = {
    "authorization" : f"Bearer {adsim_token}",
}

scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
json_file = "credentials.json"

def login():
    credentials = service_account.Credentials.from_service_account_file(json_file)
    scoped_credentials = credentials.with_scopes(scopes)
    gc = gspread.authorize(scoped_credentials)
    return gc

def find_differences(df1, df2, id_column, columns_to_check):
    """
    Find rows to update and insert by comparing two dataframes.
    
    Parameters:
        df1 (pd.DataFrame): The first dataframe (e.g., current data).
        df2 (pd.DataFrame): The second dataframe (e.g., new data).
        id_column (str): The name of the ID column to match on.
        columns_to_check (list): List of column names to compare.
        
    Returns:
        dict: A dictionary with two DataFrames:
            - "rows_to_update": Rows where values differ.
            - "rows_to_insert": Rows where IDs exist only in df2.
    """
    # Merge the dataframes
    merged = pd.merge(df1, df2, on=id_column, how='outer', suffixes=('_old', '_new'))
    
    # Rows where values differ for any column in columns_to_check
    rows_to_update = merged[
        merged[id_column].notna() &  # ID exists in both
        merged[[f"{col}_old" for col in columns_to_check]].notna().all(axis=1) &  # Values in "_old" columns are not NaN
        merged[[f"{col}_new" for col in columns_to_check]].notna().all(axis=1) &  # Values in "_new" columns are not NaN
        (merged[[f"{col}_old" for col in columns_to_check]].values != merged[[f"{col}_new" for col in columns_to_check]].values).any(axis=1)  # Any column differs
    ]

    # Rows where the ID exists only in df2 (new rows to insert)
    rows_to_insert = merged[
        merged[[f"{col}_old" for col in columns_to_check]].isna().all(axis=1) &  # "_old" columns are NaN
        merged[[f"{col}_new" for col in columns_to_check]].notna().all(axis=1)  # "_new" columns are not NaN
    ]
    
    return {
        "rows_to_update": rows_to_update[[id_column] + [f"{col}_new" for col in columns_to_check]],
        "rows_to_insert": rows_to_insert[[id_column] + [f"{col}_new" for col in columns_to_check]]
    }

def update_or_insert_rows(conn,cursor, table_name, id_column, columns_to_check, rows_to_update, rows_to_insert):
    for _, row in rows_to_update.iterrows():
        set_clause = ", ".join([f"{col} = %s" for col in columns_to_check])
        values = [row[f"{col}_new"] for col in columns_to_check] + [row[id_column]]
        sql_update = f"UPDATE {table_name} SET {set_clause} WHERE {id_column} = %s"
        try:           
            cursor.execute(sql_update, values)
            conn.commit()
            log_operation(f"succesfully updated data into {table_name}")
        except Exception as e:
            log_error_report(e)
            log_operation(f"failed to update data into {table_name}", "failed", str(e))

    for _, row in rows_to_insert.iterrows():
        columns = [id_column] + columns_to_check
        placeholders = ", ".join(["%s"] * len(columns))
        values = [row[f"{col}_new"] for col in [id_column] + columns_to_check]
        sql_insert = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        try:
            cursor.execute(sql_insert, values)
            conn.commit()
            log_operation(f"succesfully inserted data into {table_name}")
        except Exception as e:
            log_error_report(e)
            log_operation(f"failed to insert data to {table_name}", "failed", str(e))

def compare_and_update_table(cursor, conn, table_name, id_column, columns_to_check, df1, df2):
    result = find_differences(df1, df2, id_column, columns_to_check)
    update_or_insert_rows(conn,cursor, table_name, id_column, columns_to_check, result["rows_to_update"], result["rows_to_insert"])

def extract_adsim_data(url):
    # Make the API request
    response = r.get(url, headers=headers)

    # Print the response text and content type for debugging
    print(response.text)
    print(response.headers.get('Content-Type'))

    # Read the response text
    ndjson_text = response.text

    # Split the text into individual lines
    ndjson_lines = ndjson_text.strip().split('\n')

    # Parse each line as a JSON object
    data_list = []
    for line in ndjson_lines:
        if line.strip():  # Skip empty lines
            try:
                data_list.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"JSONDecodeError on line: {line}")
                print(e)

    # Convert the list of JSON objects into a DataFrame
    df = pd.DataFrame(data_list)
    return df

def main():
    try:
        df = extract_adsim_data(deals_url)
        df = df.rename(columns={'id': 'main_id'})
        log_operation("Fetch data from API", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("Fetch data from API", "failed", str(e))        

    print(df)
    try:
        pipeline = pd.json_normalize(df['pipeline'], sep='_')
        pipeline['main_id'] = df['main_id']

        df['pipeline_id'] = pipeline['id']
        pipeline = pipeline.rename(columns={'id': 'pipeline_id'})
        df = df.drop(columns=['pipeline'])

        pipeline = pipeline.drop(columns=['registerDate','lastUpdateDate','startDate','endDate','notes'])

        pipeline = pipeline.drop_duplicates(subset=['pipeline_id'])

        log_operation("pipeline dataframe, succesfully created!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("error encountered while transforming pipeline dataframe", "failed", str(e))

    try:
        creatorUser = pd.json_normalize(df['registeredByUser'],sep='_')
        responsibleUser = pd.json_normalize(df['responsibleUser'],sep='_')

        df['creatorUser_id'] = creatorUser['id']
        df['responsible_id'] = responsibleUser['id']

        df = df.drop(columns=['registeredByUser','responsibleUser'])

        users = pd.concat([creatorUser,responsibleUser])

        users = users.drop_duplicates(subset=['id'])
        users = users.rename(columns={'id': 'user_id'})

        users.head()
        log_operation("users dataframe created successfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("users dataframe creation failed!", "failed", str(e))
    
    #pipelineStep script block
    try:
        pipelineStep = pd.json_normalize(df['pipelineStep'],sep='_')

        df['pipelineStep_id'] = pipelineStep['id']
        pipelineStep = pipelineStep.rename(columns={'id': 'pipelineStep_id'})

        df = df.drop(columns=['pipelineStep'])

        pipelineStep = pipelineStep.drop_duplicates(subset=['pipelineStep_id'])

        pipelineStep = pipelineStep.drop(columns=['lastUpdateDate', 'registerDate'])

        pipelineStep.head()
        log_operation("pipelineStep dataframe created successfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("pipelineStep dataframe creation failed!", "failed", str(e))
    
    #company script block
    try:
        company = pd.json_normalize(df['company'], sep='_')
        df['company_id'] = company['id']

        df = df.drop(columns=['company'])

        company = company.rename(columns={'id': 'company_id'})
        company = company.drop_duplicates(subset=['company_id'])

        company.head()
        log_operation("pipelineStep dataframe created successfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("company dataframe creation failed!", "failed", str(e))

    #organization script block
    try:
        organization = pd.json_normalize(df['organization'],sep='_')

        df['organization_id'] = organization['id']

        organization = extract_adsim_data(organization_url)

        organization = organization.rename(columns={'id' : 'organization_id'})
        df = df.drop(columns=['organization'])

        organization_phoneNumbers = organization.explode('phoneNumbers')[['organization_id', 'phoneNumbers']]
        organization_phoneNumbers = organization_phoneNumbers.dropna(subset=['phoneNumbers'])
        organization_phoneNumbers = organization_phoneNumbers.drop_duplicates(subset=['phoneNumbers'])

        organization_emails = organization.explode('emails')[['organization_id','emails']]
        organization_emails = organization_emails.dropna(subset=['emails'])
        organization_emails = organization_emails.drop_duplicates(subset=['emails'])

        organization_company = pd.json_normalize(organization['company'], sep='_')
        organization['company_id'] = organization_company['id']

        segments = organization.explode('segments')[['segments']]
        segments = pd.json_normalize(segments['segments'], sep='_')
        organization['segment_id'] = segments['id']
        segments = segments.rename(columns={'id': 'segment_id'})
        segments = segments.dropna(subset=['segment_id'])
        segments = segments.drop_duplicates(subset=['segment_id'])

        portfolios = organization.explode('customerPortfolios')[['customerPortfolios']]
        portfolios = pd.json_normalize(portfolios['customerPortfolios'])
        organization['portfolio_id'] = portfolios['id']
        portfolios = portfolios.rename(columns={'id': 'portfolio_id', 'userEmail' : 'login'})
        portfolios = portfolios.dropna(subset=['portfolio_id'])
        portfolios = portfolios.drop_duplicates(subset='portfolio_id')

        portfolios = portfolios.merge(users[['user_id', 'login']], how='left', on='login')
        portfolios = portfolios.drop(columns=['login', 'userFullName'])

        organization = organization.drop(columns=['emails','phoneNumbers','company', 'notes', 'specialFields', 'links', 'segments', 'customerPortfolios'])
        organization = organization.drop_duplicates(subset=['organization_id'])
        organization = organization.dropna(subset=['organization_id'])

        log_operation("organization, segments, emails, phone dataframes created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("organization, segments, emails, phone dataframes dataframe creation failed!", "failed", str(e))

    #products script block
    try:
        products = df.explode('products')[['products']]

        products = pd.json_normalize(products['products'], sep='_')
        df['products_id'] = products['id']

        products = products.rename(columns={'id' : 'product_id'})
        df = df.drop(columns=['products'])

        products = products.dropna(subset=['product_id'])
        products = products.drop_duplicates(subset=['product_id'])

        products.head()
        log_operation("products dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("products dataframe creation failed!", "failed", str(e))

    #dealtype script block
    try:
        dealType = pd.json_normalize(df['dealType'], sep='_')

        df['dealType_id'] = dealType['id']
        df = df.drop(columns=['dealType'])

        dealType = dealType.rename(columns={'id' : 'dealType_id'})
        dealType = dealType.drop(columns=['company_id', 'company_name', 'company_cnpjCpf', 'company_logoUrl', 'company'])
        dealType = dealType.drop_duplicates(subset=['dealType_id'])
        dealType = dealType.dropna(subset=['dealType_id'])

        dealType.head()        
        log_operation("dealtype dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("dealtype dataframe creation failed!", "failed", str(e))

    #dues script block
    try:    
        dues = df.explode('dues')[['main_id', 'dues']]
        dues = pd.json_normalize(dues.to_dict(orient='records'))
        dues.columns = dues.columns.str.replace('dues.', '', regex=False)
        dues.columns = dues.columns.str.replace('.', '_', regex=False)
        dues.columns = dues.columns.str.replace('userId', 'user_id', regex=False)
        df = df.drop(columns=['dues'])

        dues.loc[dues['channel_id'] == 1154, 'channel_id'] = 941
        dues.loc[dues['channel_id'] == 944, 'channel_id'] = 934
        dues.loc[dues['channel_id'] == 955, 'channel_id'] = 934        

        dues = dues.drop(columns=['dealId', 'dues', 'product_name', 'product_tags', 'product_notes', 'product_value', 'product_endDate',
                                'product_isActive', 'product_companyId', 'product_isDeleted', 'product_startDate', 'product_registerDate',
                                'product_companyGroupId', 'product_lastUpdateDate', 'product_isControlQuotas', 'product_isControlBalance',
                                'product_isInformativeValue', 'product_isProposalAddItems', 'product_dealProductDiscount', 'product_dealProductQuantity',
                                'product_dealProductUnitValue', 'product_dealProductTotalValue', 'product_isUnitValueOverPiTable', 'product_isAvailableOnEmidiaPortal',
                                'product_isDigitalProposalAddItems', 'product_isProposalValueOnCurrentTable', 'product_isAutomaticDistributedScheduling', 'product_isProposalDistributeProductsByPeriod',
                                'product', 'channel', 'displayLocation', 'displayLocation_name', 'displayLocation_initials', 'channel_name', 'channel_initials'])
        dues = dues.dropna(subset=['id'])

        dues = dues.rename(columns={'id' : 'dues_id', 'userId' : 'user_id', 'companyId' : 'company_id'})

        dues.head()
        log_operation("dues dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("dues dataframe creation failed!", "failed", str(e))

    #person script block
    try:
        person = pd.json_normalize(df['person'])
        person = person.rename(columns={'id' : 'person_id'})        
        df['person_id'] = person['person_id']
        df = df.drop(columns=['person'])
        person = person.dropna(subset='person_id')
        person = person.drop_duplicates(subset=['person_id'])

        person.head()
        log_operation("person dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("person dataframe creation failed!", "failed", str(e))

    #agencies script block
    try:
        agencies = df.explode('agencies')[['agencies']]
        agencies = pd.json_normalize(agencies['agencies'], sep='_')
        agencies = agencies.rename(columns={'id': 'agencia_id'})

        df['agencies_id'] = agencies['agencia_id']
        df = df.drop(columns=['agencies'])
        agencies = agencies.dropna(subset='agencia_id')

        agencia_phoneNumbers = agencies.explode('phoneNumbers')[['agencia_id', 'phoneNumbers']]
        agencia_phoneNumbers = agencia_phoneNumbers.dropna(subset=['phoneNumbers'])

        agencia_emails = agencies.explode('emails')[['agencia_id','emails']]
        agencia_emails = agencia_emails.dropna(subset=['emails'])

        agencies['segments_id'] = np.nan
        agencies['customerPortfolios_id'] = np.nan

        agencies = agencies.drop(columns=['emails','phoneNumbers','company_name','company_cnpjCpf', 'notes', 'specialFields', 'links', 'segments', 'customerPortfolios'])
        agencies = agencies.drop_duplicates(subset=['agencia_id'])

        agencies.head()        
        log_operation("agencies dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("agencies dataframe creation failed!", "failed", str(e))

    #logs script block
    try:
        pf = extract_adsim_data(logs_url)

        print(pf)

        pf = pf.rename(columns={'dealId' : 'main_id', 'companyId' : 'company_id', 'pipelineStepid' : 'pipelineStep_id', 'pipelineId' : 'pipeline_id', 'userId' : 'user_Id'})        
        log_operation("logs data extracted succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("logs data extraction failed!", "failed", str(e))

    #excel script block
    try:
        matriz_executivos = pd.read_excel(r'matriz_executivos.xlsx')
        matriz_equipes = pd.read_excel(r'matriz_equipes.xlsx')

        users = users.merge(matriz_executivos[['login', 'equipe_id']], how='inner', on='login')

        users.head()        
        log_operation("excel data fetched succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("excel data fetch failed!", "failed", str(e))

    #proposals script block
    try:
        gf = extract_adsim_data(proposals_url)

        matriz_geotargets = pd.read_excel('IDS_TargetsDigital.xlsx')        
        log_operation("proposals and geotargets dataframes created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("proposals and geotargets dataframe creation failed!", "failed", str(e))

    #proposals_transforming script block
    try:
        gf = gf.rename(columns={'id': 'proposal_id'})
        gf_deals = pd.json_normalize(gf['deal'])
        gf['main_id'] = gf_deals['id']
        gf = gf.drop(columns=['deal'])

        items = gf.explode('items')[['main_id', 'proposal_id', 'items']]
        items = pd.json_normalize(items.to_dict(orient='records'))
        items.columns = items.columns.str.replace('items.', '', regex=False)
        items.columns = items.columns.str.replace('.', '_', regex=False)
        items = items.rename(columns={'id' : 'item_id'})
        items = items.dropna(subset=['item_id'])
        gf = gf.drop(columns=['items'])
        items = items.drop(columns=['text', 'isText'])

        gf_executives = pd.json_normalize(gf['executive'])
        gf['executive_id'] = gf_executives['id']
        gf = gf.drop(columns=['executive'])

        items_digital = gf.explode('itemsDigital')[['main_id', 'proposal_id', 'itemsDigital']]
        items_digital = pd.json_normalize(items_digital.to_dict(orient='records'))
        items_digital.columns = items_digital.columns.str.replace('itemsDigital.', '', regex=False)
        items_digital.columns = items_digital.columns.str.replace('.', '_', regex=False)
        items_digital = items_digital.rename(columns={'geotarget_name' : 'displayLocation_name', 'geotarget_initials' : 'displayLocation_initials', 'id' : 'item_id'})
        items_digital = items_digital.dropna(subset=['item_id'])
        items_digital = items_digital.merge(matriz_geotargets[['displayLocation_initials', 'displayLocation_id']], on='displayLocation_initials', how='left')
        gf = gf.drop(columns=['itemsDigital'])

        cha_cols = ['channel_id', 'channel_name', 'channel_initials']
        dis_cols = ['displayLocation_id', 'displayLocation_name', 'displayLocation_initials']
        prd_cols = ['product_id', 'product_name']
        prg_cols = ['program_id', 'program_name', 'program_initials']
        for_cols = ['format_id', 'format_name', 'format_initials']

        channels = pd.concat([items[cha_cols], items_digital[cha_cols]], axis=0, ignore_index=True)
        displayLocations = pd.concat([items[dis_cols], matriz_geotargets[dis_cols]], axis=0, ignore_index=True)
        products = pd.concat([items[prd_cols], items_digital[prd_cols]], axis=0, ignore_index=True)
        programs = items[['program_id', 'program_name', 'program_initials']].copy()
        formats = pd.concat([items[for_cols], items_digital[for_cols]], axis=0, ignore_index=True)

        drop_cols = ['channel_name', 'channel_initials', 'displayLocation_name', 'displayLocation_initials', 'format_name', 'format_initials', 'product_name']
        drop_cols1 = ['program_name', 'program_initials']
            
        items = items.drop(columns=drop_cols)
        items = items.drop(columns=drop_cols1)
        items_digital = items_digital.drop(columns=drop_cols)

        channels.loc[channels['channel_id'] == 1154, 'channel_id'] = 941
        channels.loc[channels['channel_id'] == 944, 'channel_id'] = 934
        channels.loc[channels['channel_id'] == 955, 'channel_id'] = 934

        items.loc[items['channel_id'] == 1154, 'channel_id'] = 941
        items.loc[items['channel_id'] == 944, 'channel_id'] = 934
        items.loc[items['channel_id'] == 955, 'channel_id'] = 934

        items_digital.loc[items_digital['channel_id'] == 1154, 'channel_id'] = 941
        items_digital.loc[items_digital['channel_id'] == 944, 'channel_id'] = 934
        items_digital.loc[items_digital['channel_id'] == 955, 'channel_id'] = 934

        channels = channels.drop_duplicates(subset=['channel_id'])
        displayLocations = displayLocations.drop_duplicates(subset=['displayLocation_id'])
        products = products.drop_duplicates(subset=['product_id'])
        programs = programs.drop_duplicates(subset='program_id')
        formats = formats.drop_duplicates(subset=['format_id'])

        channels = channels.dropna(subset=['channel_id'])
        displayLocations = displayLocations.dropna(subset=['displayLocation_id'])
        products = products.dropna(subset=['product_id'])
        programs = programs.dropna(subset=['program_id'])
        formats = formats.dropna(subset=['format_id'])

        items = items.drop(columns=['items'])
        items_digital = items_digital.drop(columns=['itemsDigital'])

        items = pd.concat([items, items_digital], axis=0, ignore_index=True)

        agencia_emails = agencia_emails.rename(columns={'agencia_id' : 'organization_id'})
        agencies = agencies.rename(columns={'agencia_id' : 'organization_id'})
        agencia_phoneNumbers = agencia_phoneNumbers.rename(columns={'agencia_id' : 'organization_id'})

        organization = pd.concat([agencies,organization], axis=0, ignore_index=True)
        organization_emails = pd.concat([agencia_emails,organization_emails], axis=0, ignore_index=True)
        organization_phoneNumbers = pd.concat([agencia_phoneNumbers,organization_phoneNumbers], axis=0, ignore_index=True)

        organization = organization.drop_duplicates(subset=['organization_id'])   
        matriz_geotargets.head()     
        log_operation("proposal dataframe cleaned succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("proposal dataframe cleaning failed!", "failed", str(e))

    #sales script block
    try:
        gc = login()
        planilha = gc.open("VENDAS 2025 VERSÃO EUA")
        aba = planilha.worksheet("sheet")
        dados = aba.get_all_records()
        vendas = pd.DataFrame(dados)        
        log_operation("sales dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("sales dataframe creation failed!", "failed", str(e))

    #sales tranforming script block
    try:
        users['EXECUTIVO'] = users['name'] + ' ' + users['lastname']
        users['EXECUTIVO'] = users['EXECUTIVO'].str.upper()

        vendas.loc[(vendas['EXECUTIVO'].str.contains('GESTÃO')) & (vendas['PLATAFORMA'].str.contains('JP CURITIBA')), 'EXECUTIVO'] = "BRUNO MARFURTE"
        vendas.loc[(vendas['EXECUTIVO'].str.contains('GESTÃO')) & (vendas['PLATAFORMA'].str.contains('JP CASCAVEL')), 'EXECUTIVO'] = "JOSIELI BASTIANI"
        vendas.loc[(vendas['EXECUTIVO'].str.contains('GESTÃO')) & (vendas['PLATAFORMA'].str.contains('NEWS CURITIBA')), 'EXECUTIVO'] = "BRUNO MARFURTE"
        vendas.loc[(vendas['EXECUTIVO'].str.contains('GESTÃO')) & (vendas['PLATAFORMA'].str.contains('TOPVIEW')), 'EXECUTIVO'] = "LEONARDO ZAIDAN"

        vendas['EXECUTIVO'] = vendas['EXECUTIVO'].str.strip()
        vendas = vendas.merge(users[['EXECUTIVO', 'user_id']], how='left', on='EXECUTIVO')

        users = users.drop(columns=['EXECUTIVO'])
        users = users.drop_duplicates(subset=['user_id'])

        vendas = vendas.dropna()

        vendas.loc[vendas['EXECUTIVO'].str.contains('NOVOS'), 'EXECUTIVO'] = 'GILSON BETTE'
        vendas.loc[vendas['PRAÇA'].str.contains('INSTITUC.'), 'PRAÇA'] = 'INSTITUCIONAL'
        vendas.loc[vendas['PLATAFORMA'].str.contains('GOV'), 'PLATAFORMA'] = vendas.loc[vendas['PLATAFORMA'].str.contains('GOV'), 'FONTE DE DADOS'].values
        vendas.loc[vendas['PLATAFORMA'].str.contains('WTC'), 'PLATAFORMA'] = 'DIGITAL'
        vendas.loc[vendas['PLATAFORMA'].str.contains('RIC PODCAST'), 'PLATAFORMA'] = 'DIGITAL'
        vendas.loc[vendas['PLATAFORMA'].str.contains('RIC LAB'), 'PLATAFORMA'] = 'DIGITAL'
        vendas.loc[vendas['PLATAFORMA'].str.contains('TV'), 'PLATAFORMA'] = 'RICTV RECORD'
        vendas.loc[vendas['PLATAFORMA'].str.contains('RÁDIO'), 'PLATAFORMA'] = 'JOVEM PAN PR'
        vendas.loc[vendas['PLATAFORMA'].str.contains('REVISTA'), 'PLATAFORMA'] = 'JOVEM PAN NEWS PR'
        vendas.loc[vendas['PLATAFORMA'].str.contains('RICtv'), 'PLATAFORMA'] = 'RICTV RECORD'
        vendas.loc[vendas['PLATAFORMA'].str.contains('JP'), 'PLATAFORMA'] = 'JOVEM PAN PR'
        vendas.loc[vendas['PLATAFORMA'].str.contains('NEWS'), 'PLATAFORMA'] = 'JOVEM PAN NEWS PR'
        vendas.loc[vendas['PLATAFORMA'].str.contains('DIGITAL'), 'PLATAFORMA'] = 'PORTAL ric.com.br'

        vendas['PLATAFORMA'] = vendas['PLATAFORMA'].str.strip()
        channels['channel_name'] = channels['channel_name'].str.strip()

        vendas = vendas.rename(columns={'PLATAFORMA' : 'channel_name', 'PRAÇA' : 'title'})

        vendas = vendas.merge(channels[['channel_name', 'channel_id']], how='left', on='channel_name')
        vendas = vendas.merge(pipeline[['title','pipeline_id']],how='left',on='title')

        vendas = vendas.drop(columns=['HISTÓRICO 2024', 'VIRADA', 'MÊS ANTERIOR', 'MÊS ATUAL X MÊS ANTERIOR', 
                                    'CRESCIMENTO 2025X2024', 'channel_name', 'title', 'EXECUTIVO', 'PREMIAÇÃO DIRETORIA GERAL', 'PREMIAÇÃO DIRETORIA DE PRAÇA', 
                                    'PREMIAÇÃO DIRETORIA DE PRAÇA', 'PREMIAÇÃO DIRETORIA NACIONAL', 'PREMIAÇÃO GESTOR DIGITAL', 'PREMIAÇÃO INSTITUCIONAL', 'PREMIAÇÃO GERÊNCIA',
                                    'PREMIAÇÃO INDIVIDUAL', 'PREMIAÇÃO HEAD DIGITAL', 'FORECAST 1', 'FORECAST 2'])
        
        df.loc[(df['pipeline_id'] == 1233) & (df['pipelineStep_id'] == 6865), 'isWon'] = True
        log_operation("sales dataframe transformed succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("sales dataframe transformation failed!", "failed", str(e))

    #rows_collect script block
    try:
        # Collect all historico rows as tuples
        historico_data = [
            (
                row['id'], row['enterDate'], row['pipeline_id'], row['pipelineStepId'],
                row['user_Id'], row['company_id'], row['value'], row['main_id']
            )
            for _, row in pf.iterrows()
        ]
            
        log_operation("historico dataframe created succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("-- dataframe creation failed!", "failed", str(e))

    #normalization script block
    try:
        #changing dataframe column names to lower case
        df.columns = df.columns.str.lower()
        users.columns = users.columns.str.lower()
        pipeline.columns = pipeline.columns.str.lower()
        pipelineStep.columns = pipelineStep.columns.str.lower()
        organization.columns = organization.columns.str.lower()
        organization_emails.columns = organization_emails.columns.str.lower()
        organization_phoneNumbers.columns = organization_phoneNumbers.columns.str.lower()
        dealType.columns = dealType.columns.str.lower()
        dues.columns = dues.columns.str.lower()
        company.columns = company.columns.str.lower()
        products.columns = products.columns.str.lower()
        segments.columns = segments.columns.str.lower()
        person.columns = person.columns.str.lower()
        pf.columns = pf.columns.str.lower()
        matriz_equipes.columns = matriz_equipes.columns.str.lower()
        gf.columns = gf.columns.str.lower()
        items.columns = items.columns.str.lower()
        channels.columns = channels.columns.str.lower()
        displayLocations.columns = displayLocations.columns.str.lower()
        programs.columns = programs.columns.str.lower()
        formats.columns = formats.columns.str.lower()
        portfolios.columns = portfolios.columns.str.lower()
        matriz_equipes = matriz_equipes.str.lower()

        vendas = vendas.rename(columns={'REGIÃO' : 'regiao',
                                        'AREA DE NEGÓCIO' : 'area_negocio',
                                        'MÊS/ANO' : 'mes_ano',
                                        'FONTE DE DADOS' : 'fonte_dados',
                                        'NEGÓCIO' : 'negocio',
                                        'ID POWER BI' : 'ID'})

        pipelineStep = pipelineStep.rename(columns={'pipelinestepid' : 'pipelinestep_id'})
        portfolios = portfolios.rename(columns={'companyid' : 'company_id'})

        vendas.columns = vendas.columns.str.lower()        
        log_operation("dataframe normalized succesfully!", "success")
    except Exception as e:
        log_error_report(e)
        log_operation("dataframe normalization failed!", "failed", str(e))

    try:
        # Establish connection
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        print("Connected to the database!")
        
        # Create a cursor
        cursor = conn.cursor()

        table_mappings = {
        "teams": ("equipe_id", ['equipe_name'], matriz_equipes),
        "company": ("company_id", ['name', 'cnpjcpf'], company),
        "displaylocations": ("displaylocation_id", ['displaylocation_name', 'displaylocation_initials'], displayLocations),
        "channels" : ("channel_id", ['channel_name', 'channel_initials'], channels),
        "formats" : ("format_id", ['format_name', 'format_initials'], formats),
        "programs" : ("program_id", ['program_name', 'program_initials'], programs),
        "segments" : ("segment_id", ['isactive', 'isdeleted', 'description'], segments),
        "users" : ("user_id", ['cpf', 'name', 'login', 'lastname', 'equipe_id'], users),
        "pipeline" : ("pipeline_id", ['title', 'goaldeal', 'isactive', 'isdeleted'], pipeline),
        "pipelinestep" : ("pipelinestep_id", ['title', 'goaldeal', 'isactive', 'goalvalue', 'isdeleted', 'isplanning', 'sequenceorder'], pipelineStep),
        "products" : ("product_id", ['product_name'], products),
        "organization" : ("organization_id", ['cpf', 'cnpj', 'name', 'isagency', 'registerdate', 
            'corporatename', 'stateregistration', 'municipalregistration', 
            'company_id', 'segment_id', 'portfolio_id'], organization),
        "organization_emails" : ("emails", ['organization_id', 'emails'], organization_emails),
        "organization_phonenumbers" : ("phonenumbers", ['organization_id', 'phonenumbers'], organization_phoneNumbers),
        "dealtype" : ("dealtype_id", ['isactive', 'description'], dealType),
        "person" : ("person_id", ['cpf', 'name'], person),
        "portfolio" : ('portfolio_id', ['user_id', 'description', 'enddate', 'isactive', 'startdate', 'lastupdatedate'], portfolios),
        "deals" : ("main_id", ['pipeline_id', 'creatoruser_id', 'responsible_id', 'pipelinestep_id', 'organization_id', 
            'product_id', 'dealtype_id', 'agencies_id', 'iswon', 'islost', 'enddate', 'windate', 'losedate', 'netvalue', 
            'isdeleted', 'ispending', 'startdate', 'shelvedate', 'description', 'approvaldate', 'registerdate', 'sequenceorder', 
            'conclusiondate', 'conversiondate', 'lastupdatedate', 'negotiatedvalue', 'productquantity', 'forecastsalesdate', 'isadvancedproduct', 
            'activitiesquantity', 'hasproductswithquotas', 'agencycomissionpercentage'], df),
        "dues" : ("dues_id", ['main_id', 'value', 'user_id', 'channel_id', 'duedate', 
            'netvalue', 'company_id', 'paymentdate', 'registerdate', 'lastupdatedate', 'displaylocation_id'], dues),
        "proposals" : ("proposal_id", ['registerdate', 'lastupdatedate', 'isactive', 'version', 'isapproved', 
            'isrejected', 'isapprovalrequested', 'tablevalue', 'averagediscountpercentage', 'discountpercentage', 'negotiatedvalue', 'netvalue', 'title', 'approvaldate', 
            'notes', 'description', 'rejectiondate', 'rejectionreason', 'main_id', 'executive_id'], gf),
        "proposal_items" : ("item_id", ['main_id', 'proposal_id', 'startdate', 'enddate', 'isgroupingproduct', 
            'iswithoutdelivery', 'groupidentifier', 'product_id', 'unitaryvalue', 'tablevalue', 'quantitytotal', 'discountpercentage', 'negotiatedvalue', 'productioncostvalue', 
            'isproductioncosttodefine', 'grossvalue', 'netvalue', 'isreapplication', 'distributiontype', 'quantity', 'channel_id', 'displaylocation_id', 
            'program_id', 'format_id', 'durationseconds', 'websitename', 'websiteinitials', 'device_name', 'visibility_name', 
            'issendtogoogleadmanager', 'totaltablevalue', 'nettablevalue', 'costmethod_name', 'costmethod_externalcode', 'costmethod_calculationstrategy',
            'page_name', 'producttouse_id', 'producttouse_name'], items),
        "sales" : ("id", ['regiao', 'area_negocio', 'produto', 'meta', 'realizado', 
            'porcentagem', 'mes_ano', 'origem', 'negocio', 'fonte_dados', 'user_id', 'channel_id', 'pipeline_id'], vendas)
        }

        for table_name, (id_column, columns_to_check, df) in table_mappings.items():
            sql_data = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
            try:
                compare_and_update_table(cursor, conn, table_name, id_column, columns_to_check, sql_data, df)
            except Exception as e:
                print(f"Error updating table {table_name}: {e}")

        try:
            #historico SQL
            query = """
            INSERT INTO historico (
                id, enterDate, pipeline_id, pipelinestep_id, user_id, company_id, 
                value, main_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, historico_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            log_error_report(e)
            log_operation("historico insert failed!", "failed", str(e))    

        # Close connection
        cursor.close()
        conn.close()
    except Exception as e:
        conn.rollback()
        print("Error connecting to the database:", e)
    
    save_report(report)

if __name__ == "__main__":
    main()