import logging
import requests
import pandas as pd
import time
import psycopg2
import json
import boto3
from botocore.exceptions import ClientError
import sys

def get_secret():
    #secret_name = "application_prod"
    #region_name = "us-west-2"
 
    secret_name = "application_dev"
    region_name = "us-east-2"
 
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
 
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
 
    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])

def connect_to_db():
    logger = logging.getLogger()
    #database config
    secret = get_secret()
    user_name = secret["dbuser"]
    password = secret["password"]
    rds_proxy_host = secret["host"]
    port = secret["port"]
    db_name = secret["dbname"]
    print("connecting to database")
    # rds settings
 
    try:
        connection = psycopg2.connect(host=rds_proxy_host, user=user_name, password=password, dbname=db_name, port=port)
        logger.info("SUCCESS: Connection to RDS Postgres instance succeeded")
        print("connected to database")
        return connection
 
    except psycopg2.Error as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit(1)

def setup_publications(pubmed_publications, pmc_publications):
    #conn = psycopg2.connect(host=DB_CONFIG['host'], database=DB_CONFIG['database'], user=DB_CONFIG['user'], password=DB_CONFIG['password'])
    conn = connect_to_db()
    cur = conn.cursor()
    query = """
        SELECT 
            vs.phs,
            spv.study_id,
            spv.property_value as publication_url
        FROM
            public.study_property_value spv 
                join view_study vs
                    on vs.study_id = spv.study_id
        WHERE
            entity_property_id = 97
        ORDER BY id ASC
    """
    cur.execute(query)
    results = cur.fetchall()
    conn.close()
    # base dataframe with the query results from DB
    pubs_df = pd.DataFrame(data=results, columns=['PHSID', 'ID', 'URL'])
    system = []

    # we need to go row by row determining which system the URL is for
    # determines which API we use
    for index, pub in pubs_df.iterrows():
        url = pub[2]
        if (url[8:14] == 'pubmed'):
            system.append('PubMed')
        elif(url[8:16] == 'www.ncbi'):
            system.append('PubMedCentral')
        else:
            system.append('Other')
    pubs_df['System'] = system
    api_id = []

    # we need to go row by row and extract the ID from the URL
    # ID needs to be passed to the API to get info back
    for index, pub in pubs_df.iterrows():
        url = pub[2]
        system = pub[3]
        if system == 'PubMed':
            api_id.append(url[len(url)-9:len(url)-1])
        elif len(url) == 53:
            api_id.append(url[len(url)-8:len(url)-1])
        else:
            api_id.append(url[len(url)-9:len(url)-1])
    pubs_df['API_ID'] = api_id

    # now we create the separate lists for pubmed and pmc
    for index, pub in pubs_df.iterrows():
        phs = pub[0]
        system = pub[3]
        api_id = pub[4]
        if system == 'PubMed':
            pubmed_publications.append({'id': api_id, 'phs': phs})
        elif system == 'PubMedCentral':
            pmc_publications.append({'id': api_id, 'phs': phs})

def get_pmc_publications(df, pmc_publications):
    print('Getting data for given PMC IDs...')
    for publication in pmc_publications:
        id = publication['id']
        time.sleep(1.5)
        res = requests.get(f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pmc&id={id}&retmode=json')
        res = res.json()
        print(f'id - {id}')
        result = res['result']
        detail = result[id]
        articleIds = detail['articleids']
        pubmedId = articleIds[0]
        doi = ''
        if(len(articleIds) > 1):
            doi = articleIds[1]
        author_list = []
        for author in detail['authors']:
            author_list.append(author['name'])
        addRow = [publication['phs'], detail['title'], ', '.join(author_list), detail['pubdate'], pubmedId['value'], detail['fulljournalname'], doi['value']]
        df.loc[len(df.index)] = addRow

    print('Finished Collecting data for PubMed Central...')

def get_pubmed_publications(df, pubmed_publications):
    print('Getting data for given PubMed IDs...')
    for publication in pubmed_publications:
        id = publication['id']
        time.sleep(1.5)
        res = requests.get(f'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id={id}&retmode=json')
        res = res.json()
        print(f'id - {id}')
        result = res['result']
        detail = result[id]
        articleIds = detail['articleids']
        pubmedId = articleIds[0]
        doi['value'] = ''
        if(len(articleIds) > 1):
            doi = articleIds[1]
        author_list = []
        for author in detail['authors']:
            author_list.append(author['name'])
        addRow = [publication['phs'], detail['title'], ', '.join(author_list), detail['pubdate'], pubmedId['value'], detail['fulljournalname'], doi['value']]
        df.loc[len(df.index)] = addRow

    print('Finished Collecting data for PubMed...')

def run_publications():
    # final publicaitons dataframe with all data in it
    publications = pd.DataFrame(columns=['PHS ID', 'Title', 'Authors', 'Publication Date', 'PubMed ID', 'Journal', 'DOI'])
    pubmed_publications = []
    pmc_publications = []
    setup_publications(pubmed_publications, pmc_publications)
    get_pmc_publications(publications, pmc_publications)
    get_pubmed_publications(publications, pubmed_publications)
    publications.to_excel('/Users/patricklenhart/Documents/RADx/RADxPublications.xlsx', index=False)
    #uploads excel to temp file
    file_name = f"RADxPublications.xlsx"
    file_path = f"/tmp/{file_name}"
    with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
        workbook = writer.book
        publications.to_excel(writer, sheet_name="Publications", header=True, index=False)
    s3 = boto3.client('s3')
    with open(file_path, "rb") as file:
        s3.upload_fileobj(file, "dev.radxdatahub.nih.gov", f"radx-s3-resources/{file_name}")

def lambda_handler(event, context):
    try:
        run_publications()
    except Exception as e:
        return {
            'statusCode': 400,
            'body' : json.dumps(f"ERROR: An error occurred: {e}")
        }
    return {
        'statusCode': 200,
        'body': json.dumps('RADx Content Report Uploaded Successfully!')
    }

