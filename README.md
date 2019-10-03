# tap_freshsales

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

Build Status: [![CircleCI](https://circleci.com/gh/Lori-Systems/tap_freshsales/tree/master.svg?style=svg)](https://circleci.com/gh/Lori-Systems/tap_freshsales/tree/master)

This tap:

- Pulls raw data from [FreshSales REST API](https://www.freshsales.io/api)
- Extracts the following resources:
  - [Leads](https://www.freshsales.io/api/#leads)
  - [Contacts](https://www.freshsales.io/api/#contacts)
  - [Accounts](https://www.freshsales.io/api/#accounts)
  - [Deals](https://www.freshsales.io/api/#deals)
  - [Tasks](https://www.freshsales.io/api/#tasks)
  - [Appointments](https://www.freshsales.io/api/#appointments)
  - [Sales](https://www.freshsales.io/api/#sales-activities)
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
![cropped 1](https://user-images.githubusercontent.com/8224798/65393181-25244180-dd86-11e9-8130-eaa6fd9e1021.png)

  - STATE : `{ item_id: updated_at }` - Adds the bookmapping function where only sync if a record has been updated from previous sync.
  - SCHEMA : Generated schema - automates the `Data insert ` process
  - RECORD : Actual data for each record in json format.

# Running tapo to Postgres Database
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
---

Copyright &copy; 2018 Lori
