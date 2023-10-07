#!/bin/bash

if [ -n $PORT ]; then
   PORT=3306
fi

if [ -n $TUNER_DIR ]; then
   echo "TUNER_DIR is not, so defaulting to current dir `pwd`"
   TUNER_DIR=`pwd`
fi

# check if the TUNER_DIR is a valid dir.
if ! [ -f $TUNER_DIR/tuner.py -o -f $TUNER_DIR/tuner_gpt4.py ]; then
   echo "Invalid TUNER_DIR:$TUNER_DIR  as it doesn't contain tuper.py or tuner_gpt4.py"
   exit 0
fi

# check if the tuner dir contains the tpch queries and tpch data to load.
if ! [ -d $TUNER_DIR/data -o -d $TUNER_DIR/tpch-queries ]; then
   echo "Invalid TUNER_DIR:$TUNER_DIR as it doesn't contain data or tpch-queries directory"
   exit 0
fi

# check if there is a schema.sql file 
if ! [ -f $TUNER_DIR/tpch-queries/schema.sql ]; then
   echo "No schema.sql file exists under $TUNER_DIR"
   exit 0
fi


# check if there is a load.sql file 
if ! [ -f $TUNER_DIR/tpch-queries/load.sql ]; then
   echo "No load.sql file exists under $TUNER_DIR"
   exit 0
fi

#create the tuner_db database.
mysql --host 127.0.0.1 --port $PORT -u root < $TUNER_DIR/tpch-queries/create_db.sql

# create the tables in the tuner_db
mysql tuner_db --host 127.0.0.1 --port $PORT -u root < $TUNER_DIR/tpch-queries/schema.sql

#pre process the load.sql to use the right directory in the load
ESC_TUNER_DIR=$(printf '%s\n' "$TUNER_DIR" | sed -e 's/[\/&]/\\&/g' | sed 's/\./\\./g')
sed "s/<TUNER_DIR>/$ESC_TUNER_DIR/g" $TUNER_DIR/tpch-queries/load.sql > /tmp/load.sql

# load the data into the tables.
mysql tuner_db --local-infile=1  --host 127.0.0.1 --port $PORT -u root < /tmp/load.sql

