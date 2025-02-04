import zipfile
import openai
import os

openai.api_type = "azure"
openai.api_base = "https://zhoushuai-test.openai.azure.com/"
openai.api_version = "2024-10-21"

from mysql.connector import connect, MySQLConnection
from mysql.connector.cursor import MySQLCursor
from collections import defaultdict

def is_valid_zip(file_path):
    """Check if the given file is a valid ZIP archive."""
    try:
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            return zip_ref.testzip() is None  # Returns True if no corrupted files
    except zipfile.BadZipFile:
        return False
    except FileNotFoundError:
        return False

def read_file_contents(file_name: str) -> str:
    """Reads the contents of a file and returns it as a string."""
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        return f"Error: The file '{file_name}' was not found."
    except Exception as e:
        return f"Error: {e}"

def apply_one_analysis(prompt_value, openai):
    message = [{"role": "user", "content": prompt_value}]
    response = openai.ChatCompletion.create(engine="test-gpt-4",
                                            messages=message,
                                            temperature=0.7,
                                            max_tokens=800,
                                            top_p=0.95,
                                            frequency_penalty=0,
                                            presence_penalty=0,
                                            stop=None)
    # Print response
    print("\n \n ------ \n", response.choices[0].message.content, "\n")
    
def main():
    openai.api_key = os.getenv("OPENAI_API_KEY")
    file_name = input("Enter the ZIP file name: ")
    if not is_valid_zip(file_name):
        print(f"'{file_name}' is not a valid ZIP file or is corrupted.")
        return

    cmd = "unzip -o " + file_name + " -d query_info"
    os.system(cmd)
    plan = read_file_contents("./query_info/explain.txt")
    sql_files = sorted(os.listdir("./query_info/sql"))
    
    sql_contents = {sql_file: read_file_contents(f"./query_info/sql/{sql_file}") for sql_file in sql_files if sql_file.endswith(".sql")}
    
    prompt_values = [
        f"show hot spots in the plan+SQL and be brief and do not include anything else other than hotspots. Plan: {plan}, SQL: {sql_contents[sql_file]}"
        for sql_file in sql_contents
    ]
    
    for prompt in prompt_values:
        apply_one_analysis(prompt, openai)
    
    cmd = "rm -r ./query_info"
    os.system(cmd)
    
if __name__ == '__main__':
    main()
