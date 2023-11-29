import sqlite3 as lite
import argparse
import os
import sys
from urllib.request import pathname2url
import ctypes
import json


def analyze_workload(cur):
        cur.execute("drop table if exists unique_queries")
        cur.execute("""
            create table unique_queries(
                sql_statement text,
                frequency decimal,
                total_query_time decimal,
                max_query_time decimal,
                min_query_time decimal,
                query_type tinyint,
                number_of_joins tinyint)
        """)
        insert_statement = """
            INSERT INTO unique_queries(sql_statement, frequency, total_query_time, max_query_time, min_query_time)
            SELECT sql_statement, count(*) as frequency,
                   sum(CAST(query_time as decimal)) as total_query_time,
                   max(CAST(query_time as decimal)) as max_query_time,
                   min(CAST(query_time as decimal)) as min_query_time
            FROM slowlog
            GROUP BY sql_statement
        """
        cur.execute(insert_statement)
        log_count = cur.execute("select count(*)  from slowlog")
        print("\n log_count = ", log_count.fetchall())
        log_count = cur.execute("select sum(frequency) from unique_queries")
        print("\n log_count = ", log_count.fetchall())
        lib = ctypes.CDLL('./analyze.so')
        lib.analyze.restype = ctypes.c_char_p
        key_string = lib.analyze(insert_statement.encode("utf-8"), True)
        keys = json.loads(key_string.decode("utf-8"))
        print("\n keys = ", keys[0], key_string, ",join_count = ", 0)

def main():
    parser = argparse.ArgumentParser(description='Workload analysis.')
    parser.add_argument('--test_database',  help='sqllite name with log data')
    args = parser.parse_args()
    test_database = args.test_database

    if test_database != "":
        try:
            dburi = 'file:{}?mode=rw'.format(pathname2url(test_database))
            con = lite.connect(dburi, uri=True)
        except lite.OperationalError:
            print("\n incorrect database name:",test_database)
            sys.exit()
        cur = con.cursor()
        analyze_workload(cur)
        con.commit()
    else:
        print ("usage: python3 workload_analysis.py --test_database database_name")

if __name__ == '__main__':
    main()
