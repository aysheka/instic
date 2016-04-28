# instic
Create/restore dump utils for cassandra

## About

Inspired by current project [gianlucaborello/cassandradump](https://github.com/gianlucaborello/cassandradump) but written on java and fix some problems with UDT and performance 

It's very bad idea to create dump for cassandra because such DB was not developed for current operation and it can store PB
 of data, so creating dump in single file on single machine is very bad idea. But current tools is good for dev environment to boot up some fixtures.
 
 

## Configuration

Edit application properties file (application.json)

Where

```json
{
  "cassandra": {
    "host": "casasndrahost1,casasndrahost1",
    "port": 9042,
    "keyspace": "keyspace that for export/import",
    "username": "username",
    "password": "password"
  },
  "working_dir": "path to dir to which will be written data or from which data will be imported",
  "export": {
    "ddl": true, // dump schema on export operation
    "dml": true  // dump data on export operation
  },
  "import": {
    "ddl": false, // restore schema on import operation. (Note that by default `system` keyspace will be used so user must hava enogh grants)
    "dml": true   // restore schema on import operation
  }
}
```

## Export
```bash
$>sh export.sh
$>ls -la [working_dir]
[keyspace].ddl
[keyspace].dml
```


## Import
```bash
$>sh import.sh
```
