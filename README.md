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
python3 -u tuner.py | tee /tmp/tuner.out
```

## python dependencies.
Following modules needs to be installed for python

```
pip3 install mysql-connector-python
pip3 install openai
```
