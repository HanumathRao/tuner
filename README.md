# Commands to run

## create the table schema.
```
mysql test --host 127.0.0.1 --port 4000 -u root < ~/tuner/tpch-queries/schema.sql
```

## load the table
```
mysql test --local-infile=1  --host 127.0.0.1 --port 4000 -u root < ~/tuner/tpch-queries/load.sql
```

## Alternate easy way of creating the database and loading the tables.
```
load_data.sh
```

## run the tuner
```
python3 tuner.py test_file --singletest
to run a tuner on a single query. Note that:
- TiDB is running and schema used in the query exists in the database.
- database used is tuner_db. Modify it if it is different
- PORT is set to 4000 and modify it accordingly.

regression tests
python3 tuner.py <test-directory>  
This just tests query markers (does it have joins, filters, ...) and test which rewrite are applicable
so far we have two regression tests: tpch_queries and sample_queries 

```

## python dependencies.
Following modules needs to be installed for python

```
pip3 install mysql-connector-python
pip3 install openai
```

## compiling analyze.go file.

```
go build -o analyze.so -buildmode=c-shared analyze_sql.go
```
