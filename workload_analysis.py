import sqlite3 as lite
import argparse
import os
import sys
from urllib.request import pathname2url
import ctypes
import json
import re
import math

lib = ctypes.CDLL('./analyze.so')
lib.analyze.restype = ctypes.c_char_p
def analyze_one_query(sql):
        #few hacks to clean up sensitized SQL
        sql = sql.replace('\n',' ')
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
        key_string = (lib.analyze(sql.encode("utf-8"), True))
        key_string = json.loads(key_string.decode("utf-8"))
        if (len(key_string) == 0):
            if re.search('.*insert.*values.*',sql):
                key_string = ['Insert']
            elif re.search('^analyze.*',sql):
                key_string = ['Analyze']
            else:
                key_string = ['None']
        key_string = str(key_string).replace("'","")
        return key_string

def number_of_joins(markers):
        num = markers.count('InnerJoin')+markers.count('LeftJoin')+markers.count('RightJoin')
        return num

def number_of_aggregate(markers):
        return markers.count('Aggregate')

def number_of_groupby(markers):
        return markers.count('GroupBy')+markers.count('Distinct')

def query_type(markers):
        if (markers == '[Insert]'):
            return 'INSERT_VALUES'
        elif (markers.count('Delete') == 1 and markers.count('Select') == 0):
            return 'DELETE'
        elif (markers.count('Update') == 1 and markers.count('Select') == 0):
            return 'UPDATE'
        elif (markers == '[Analyze]'):
            return 'ANALYZE'
        elif (markers == '[None]'):
            return 'UNKNOWN'
        elif (markers.count('Explain') == 1):
            return 'EXPLAIN'
        elif (markers.count('System') == 1):
            return 'SYSTEM'
        else:
            num_agg = number_of_aggregate(markers)
            num_groupby = number_of_groupby(markers)
            num_join = number_of_joins(markers)
            if (num_agg == 0 and num_groupby == 0 and num_join == 0):
                return 'SCAN'
            elif (num_agg > 0 and num_groupby == 0 and num_join == 0):
                return 'AGGREGATE_SCAN_NO_GROUPBY'
            elif (num_agg > 0 and num_groupby == 0 and num_join > 0):
                return 'AGGREGATE_JOIN_NO_GROUPBY'
            elif (num_groupby > 0 and num_join == 0):
                return 'AGGREGATE_SCAN_GROUPBY'
            elif (num_groupby > 0 and num_join > 0):
                return 'AGGREGATE_JOIN_GROUPBY'
            elif (num_groupby == 0 and num_agg == 0 and num_join > 0):
                return 'JOIN_NO_AGGREGATE'
            else:
                return 'FIX-IT'

def report_by_query_resource(cur, time_or_mem):
        column1 = 'Query Type'
        if time_or_mem == 'time':
            column2 = 'Total Query Time in Seconds'
            report_by_query_resource_cur = cur.execute("""
                select query_type, sum(total_query_time) from unique_queries group by query_type order by sum(total_query_time) desc
            """)
        elif time_or_mem == 'memory':
            column2 = 'Total MB Memory'
            report_by_query_resource_cur = cur.execute("""
                select query_type, sum(total_mem) from unique_queries group by query_type order by sum(total_mem) desc
            """)
        else:
            print("\n invalid choice in report_by_query_resource ")
            return
        one_row = report_by_query_resource_cur.fetchone() 
        if one_row is not None:
            print("\n")
            print(f'{column1:<35}', f'{column2:<20}')
        while one_row is not None:
            total_query_resource = math.ceil(one_row[1]*100)/100
            if time_or_mem == 'memory':
                total_query_resource = total_query_resource/1024.00/1024.00
            print(f'{one_row[0]:<35}', f'{total_query_resource:<20}')
            one_row = report_by_query_resource_cur.fetchone() 

def report_by_frequency(cur):
        report_by_frequency_cur = cur.execute("""
            select query_type, sum(frequency) from unique_queries group by query_type order by sum(frequency) desc
        """)
        one_row = report_by_frequency_cur.fetchone() 
        if one_row is not None:
            print("\n")
            column1 = 'Query Type'
            column2 = 'Frequency'
            print(f'{column1:<35}', f'{column2:<10}')
        while one_row is not None:
            print(f'{one_row[0]:<35}', f'{one_row[1]:<10}')
            one_row = report_by_frequency_cur.fetchone() 

def read_vs_write_report(cur):
        report_cur = cur.execute("""
            select query_markers, frequency, total_query_time, total_mem  from unique_queries
        """)
        one_row = report_cur.fetchone() 
        if one_row is not None:
            print("\n")
            column1 = 'Query Type'
            column2 = 'Frequency'
            column3 = 'Total Time'
            column4 = 'Total MB Memory'
            write = 'Write'
            read = 'Read'
            read_freq = 0
            read_total_time = 0.0
            read_total_mem = 0.0
            write_freq = 0
            write_total_time = 0.0
            write_total_mem = 0.0
            print(f'{column1:<35}', f'{column2:<15}', f'{column3:<15}', f'{column4:<15}')
        else:
            return
        while one_row is not None:
            markers = one_row[0]
            if (markers.count('Delete') > 0 or markers.count('Insert') > 0 or markers.count('Update') > 0):
                write_freq = write_freq+one_row[1]
                write_total_time = write_total_time+one_row[2]
                write_total_mem = write_total_mem+one_row[3]
            elif (markers.count('Select') > 0):
                read_freq = read_freq+one_row[1]
                read_total_time = read_total_time+one_row[2]
                read_total_mem = read_total_mem+one_row[3]
            one_row = report_cur.fetchone() 
        write_total_time = math.ceil(write_total_time)*100/100
        write_total_mem = math.ceil(write_total_mem/1024.0/1024.0)*100/100
        read_total_time = math.ceil(read_total_time)*100/100
        read_total_mem = math.ceil(read_total_mem/1024.0/1024.0)*100/100
        print(f'{read:<35}', f'{read_freq:<15}', f'{read_total_time:<15}', f'{read_total_mem:<15}')
        print(f'{write:<35}', f'{write_freq:<15}', f'{write_total_time:<15}', f'{write_total_mem:<15}')


def analyze_workload(con):
        con.create_function("analyzeOneQuery", 1, analyze_one_query)
        con.create_function("numberOfJoins", 1, number_of_joins)
        con.create_function("queryType", 1, query_type)
        cur = con.cursor()
        cur.execute("drop table if exists unique_queries")
        cur.execute("""
            create table unique_queries(
                sql_statement text,
                frequency decimal,
                total_query_time decimal,
                max_query_time decimal,
                min_query_time decimal,
                total_mem decimal,
                max_mem decimal,
                min_mem decimal,
                query_markers text,
                number_of_joins tinyint,
                query_type text)
        """)
        insert_statement = """
            INSERT INTO unique_queries(
                sql_statement, frequency,
                total_query_time, max_query_time, min_query_time,
                total_mem, max_mem, min_mem,
                query_markers, number_of_joins, query_type
            )
            SELECT sql_statement, count(*) as frequency,
                   sum(CAST(query_time as decimal)) as total_query_time,
                   max(CAST(query_time as decimal)) as max_query_time,
                   min(CAST(query_time as decimal)) as min_query_time,
                   sum(CAST(mem_max as decimal)) as total_mem,
                   max(CAST(mem_max as decimal)) as max_mem,
                   min(CAST(mem_max as decimal)) as min_mem,
                   analyzeOneQuery(sql_statement),
                   numberOfJoins(analyzeOneQuery(sql_statement)),
                   queryType(analyzeOneQuery(sql_statement))
            FROM slowlog
            GROUP BY sql_statement
            order by max_query_time desc
        """
        cur.execute(insert_statement)
        report_by_frequency(cur)
        report_by_query_resource(cur,'time')
        report_by_query_resource(cur,'memory')
        read_vs_write_report(cur)

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
        analyze_workload(con)
        con.commit()
        con.close()
    else:
        print ("usage: python3 workload_analysis.py --test_database database_name")

if __name__ == '__main__':
    main()
