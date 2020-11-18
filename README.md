# PostgreSQL Wal input plugin for Embulk

Fetch WAL from PostgreSQL
Minimum support version of PostgreSQL is 9.4 for [Logical replication API](https://jdbc.postgresql.org/documentation/head/replication.html)

## Overview

* **Plugin type**: input
* **Resume supported**: no
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration
- **host**: database host name (string, required)
- **port**: database port number (integer, default: 5432)
- **user**: database login user name (string, required)
- **password**: database login password (string, default: "")
- **database**: destination database name (string, required)
- **schema**: destination schema name (string, default: "public")
- **table**: destination table name (string, required)
- **slot**: replication slot name (string, required)
- **from_lsn**: start from this LSN in the slot (string, optional)
- **to_lsn**: stop retrieving data if LSN is exceeded (string, optional)
- **default_timezone**: If the sql type of column is `date`/`time`/`datetime` and the embulk type is `string`, column values are formatted into this default_timezone. You can overwrite timezone for each columns using column_options option. (string, default: `UTC`)
- **options**: extra connection properties (hash, default: {})
- **enable_metadata_deleted**: flag to add metadata deleted to each row (bool, default: `true`)
- **enable_metadata_fetched_at**: flag to add metadata synced_at to each row (bool, default: `true`)
- **enable_metadata_seq**: sequence number of record (bool, default: `true`)
- **metadata_prefix**: metadata prefix (string, default: `_`)
- **columns**: MySQL column
    - name: name of the column
    - type: data type of the column
    - format: timestamp format

## Example

```yaml
in:
  type: postgresql_wal
  host: host
  port: 5432
  user: user
  password: password
  database: postgres
  schema: public
  slot: slot
  table: table
  columns:
    - name: id
      type: long
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
