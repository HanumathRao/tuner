import sqlite3 as lite
import argparse
import os
import sys
from urllib.request import pathname2url
import ctypes
import json
import re

def analyze_workload(cur):
        lib = ctypes.CDLL('./analyze.so')
        lib.analyze.restype = ctypes.c_char_p
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
            order by max_query_time desc
        """
        cur.execute(insert_statement)
        log_count = cur.execute("select count(*)  from slowlog")
        print("\n log_count = ", log_count.fetchall())
        log_count = cur.execute("select count(*)  from slowlog where sql_statement != ''")
        print("\n log_count = ", log_count.fetchall())
        log_count = cur.execute("select count(*) from unique_queries")
        print("\n total unique queries = ", log_count.fetchall())
        log_count = cur.execute("select sum(frequency) from unique_queries")
        print("\n total freq= ", log_count.fetchall())

        unique_queries_cur = cur.execute("select sql_statement from unique_queries")
        sql = list(unique_queries_cur.fetchone())
        while sql is not None:
            #few hacks to clean up sensitized SQL
            sql = sql[0].replace('\n',' ')
            sql = sql.strip()
            sql = sql.replace(" (","(")
            sql = sql.replace(" . ",".")
            sql = sql.replace("`","")
            sql = sql.replace("..."," ? ")
            sql = sql.replace("is not ?","is not true")
            sql = sql.replace("concat( ... )","concat(123)")
            sql = sql.replace(";","")
            sql = sql.replace("format = ?"," ")
            sql = sql.replace("year_month","xxx")
            sql = sql.replace("as rank ","as rank1 ")
            sql = sql.replace("as project name","as project_name")
            sql = sql.replace("as primary language","as primary_language")
            sql = sql.replace(" as ? "," ")
            print("\n inside unique_queries_cur \n")
            key_string = (lib.analyze(sql.encode("utf-8"), True))
            key_string = json.loads(key_string.decode("utf-8"))
            if (len(key_string) == 0):
                if re.search('.*insert.*values.*',sql):
                    key_string = ['Insert']
                elif re.search('^analyze.*',sql):
                    key_string = ['Analyze']
            print("\n key_string= ",key_string,"length = ",len(key_string), "type(key_string)=", type(key_string)," SQL = ", sql,"\n-------- \n")
            sql = unique_queries_cur.fetchone()

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
