# tap_freshsales

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

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

---

Copyright &copy; 2018 Lori
