# RADx-PublicationScript

A Python script that fetches study PHS numbers from the Postgres database and uses the ID to identify whether they belong to PubMed or PubMed Central (PMC). 

Calls the NCBI utilities API to fetch publication metadata, compiles and format the results into an Excel file, and uploads the file to the AWS S3 bucket for the List of Publications helpful information resource card. 

This script is currently run locally, but could be executed via AWS Lambda with the correct credentials.

## Requirements
**Python Libraries**

The script uses the following packages:
* boto3
* pandas
* requests
* psycopg2
* openpyxl
* logging
* json
* time
* sys

**AWS Services**
* Secrets Manager: Stores DB credentials
* RDS: DB instance
* S3: Stores Excel file

**AWS DB Credentials**
The script expects the json-formatted secret credentials for the DB instance
```
{
  "dbuser": "username",
  "password": "password",
  "host": "your-rds-host",
  "port": 5432,
  "dbname": "your-database"
}
```


