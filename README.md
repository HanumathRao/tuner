# Commands to run

## create the table schema.
```
mysql test --host 127.0.0.1 --port 4000 -u root < ~/tuner/tpch-queries/schema.sql
```

## load the table
```
mysql test --local-infile=1  --host 127.0.0.1 --port 4000 -u root < ~/tuner/tpch-queries/load.sql
```

## run the tuner
```
python3 tuner.py | tee /tmp/tuner.out
```
