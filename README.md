# tap_freshsales 

(freshsales crm is now freshworks)

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Build Status: [![CircleCI](https://circleci.com/gh/Lori-Systems/tap_freshsales/tree/master.svg?style=svg)](https://circleci.com/gh/Lori-Systems/tap_freshsales/tree/master)

This tap:

- Pulls raw data from [FreshSales REST API](https://www.freshsales.io/api)
- Extracts the following resources:
  - [Contacts](https://developers.freshworks.com/crm/api/#contacts)
  - [Accounts](https://developers.freshworks.com/crm/api/#accounts)
  - [Deals](https://developers.freshworks.com/crm/api/#deals)
  - [Tasks](https://developers.freshworks.com/crm/api/#tasks)
  - [Appointments](https://developers.freshworks.com/crm/api/#appointments)
  - [Sales](https://developers.freshworks.com/crm/api/#sales-activities)
- Outputs the schema for each resource
- Pulls all data

# Installation
- Clone the app
- Create virtual env `virtualenv test` then `source test/bin/activate`
- Install dependencies i.e `pip install -r requirements.txt`
- Run Tests `pytest tests`

# How to run the tap
- Create a config file from the `sample_config` already provided
```
{
  "api_key": "your-api-token",
  "domain": "subdomain",
  "start_date": "2018-11-26T00:00:00Z"
}
```
- Run the command below
```
python test/bin/tap_freshsales --config ../config.json >> ../state.json
```
- Where state.json - a file where the tap writes all data pulled from freshsales.
If successful, state.json should have this format.
  - STATE : `{ item_id: updated_at }` - Adds the bookmapping function where only sync if a record has been updated from previous sync.
  - SCHEMA : Generated schema - automates the `Data insert ` process
  - RECORD : Actual data for each record in json format.

# Running tap to Postgres Database
- To push data from tap_freshsale to postgres db using the target-postgres
- Add db_config 
```
{

    "host": "localhost",
    "port": 5432,
    "dbname": "local freshsales db",
    "user": "db user",
    "password": "db password",
    "schema": "tap_freshsales"
  }
```
- Run this command `python your_virtual_env/bin/tap_freshsales --config ../config.json | your_virtual_env/bin/target-postgres  -c ../db_config.json`

# Running tap to Redshift Database
- To push data from tap_freshsale to Redshift db using the target-redshift
- `Pip install target-redshift`
- Add db_config 
```
{
    "redshift_username": "username",
    "redshift_schema": "tap_freshsales",
    "redshift_host": "your redshift.amazonaws.com",
    "redshift_password": "password",
    "redshift_port": 5439,
    "redshift_database": "your db",
    "default_column_length": 1000,
    "target_s3":{
      "aws_access_key_id": "AKIA...",
      "aws_secret_access_key": "secrete key",
      "bucket": "bucket name",
      "key_prefix": "temp_"

    }
  }
```
- Run this command `python your_virtual_env/bin/tap_freshsales --config ../config.json | your_virtual_env/bin/target-redshift  -c ../db_config.json`

Since python2.7 has been deprecated, you might have to 
- `pip3 install target-redshift` or copy the target-redshift file to `your_env/bin/`
---
## Versions:

Copyright &copy; 2018 Lori
