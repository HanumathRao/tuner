import os
import openai
import uuid
from typing import List
import mysql.connector
import time
import ctypes
import json

from mysql.connector import connect, MySQLConnection
from mysql.connector.cursor import MySQLCursor
from collections import defaultdict

def get_connection(autocommit: bool = True) -> MySQLConnection:
    connection = connect(host='127.0.0.1',
                         port=4000,
                         user='root',
                         password='',
                         database='test')
    connection.autocommit = autocommit
    return connection

def read_file_lines(file_path):
    prompt_dict = defaultdict(list)
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            for prompt in data["prompts"]:
                for operator in prompt["operators"]:
                    prompt_dict[operator].append(prompt["prompt"])
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    return prompt_dict

def read_string_from_file(file_path):
    try:
        with open(file_path, 'r') as file:
            file_contents = file.read()
        return file_contents
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

schema = read_string_from_file("tpch-queries/schema.sql")

querymap = {}
for i in range(1, 19):
    query = read_string_from_file(f"tpch-queries/{i}.sql")
    querymap[i] = query

rewrites = read_file_lines('prompts.txt')

def total_cost(steps) -> float:
    return sum(float(value[2]) for value in steps)

def get_cost(sql) -> float:
    try:
        connection = get_connection(autocommit=True)
        cursor = connection.cursor()
        cursor.execute("explain format=verbose " + sql)
        result = cursor.fetchall()
        return total_cost(result)
    except Exception as error:
        #print(error)
        return -1.0

openai.api_key = os.getenv("OPENAI_API_KEY")
print ("\n ----------------- results -------------------- \n")

def applicable_rewrites():
    return [prompt for list in rewrites.values() for prompt in list]

def apply_rewrite(sql, rw):
    skip_gpt = True
    prompt_value="Assuming the following MySQL tables, with their properties:\n#\n#"+schema+"\n#\n### "+rw+sql+" \n"
    time.sleep(3)
    message=[{"role": "user", "content": prompt_value}]
    print ("\n ---------------------------------------------- \n")
    #print ("prompt_value = ", prompt_value)
    print ("\n ---------------------------------------------- \n")
    if skip_gpt is True:
        return
    response = openai.ChatCompletion.create(
        model="gpt-4",messages=message,temperature=0,max_tokens=1000
    )
    print ("\n ---------------------------------------------- \n")
    print ("response=",response)
    print ("\n ---------------------------------------------- \n")
    #TODO: fix code below to extract new SQL if any
    new_sql = (response.choices[0].message)
    original_cost = get_cost(sql)
    new_cost = get_cost(new_sql)
    if new_cost > 0 and new_cost < original_cost:
        #print("QUERY =", i, " ORIGINAL COST = ", original_cost, " NEW COST = ", new_cost, " PROMPT : ", rw)
        print ("ORIGINAL SQL = ",sql, " WITH COST = ", original_cost, "\n REWRITTEN SQL= ",new_sql, " WITH COST= ", new_cost)

for i in range(1, 19):
    if i == 15:
        continue
    sql = querymap[i]
    print ("SQL = ", sql)
    sql = sql.replace('\n',' ')
    lib = ctypes.CDLL('./analyze.so')
    lib.analyze.restype = ctypes.c_char_p
    print(lib.analyze(sql.encode("utf-8")))
    for rw in applicable_rewrites():
        print("rewrite: " + rw)
        apply_rewrite(sql, rw)
