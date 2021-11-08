import sqlite3
from pprint import pprint
import pandas as pd

# (MacOs)
#   python3 -m pip install pandas
# (Windows)
#   pip install pandas

# We open a connection to the SQLite database file.
# If the given database name does not exist then this call will create the database.
# isolation_level=None -> Auto-commit
dbase = sqlite3.connect('project_database.db', isolation_level=None)
print('Database opened')

# When the above program is executed,
# it will create the employee_records table in your project_database.db
dbase.execute(''' CREATE TABLE IF NOT EXISTS employee_records(
    ID INT PRIMARY KEY NOT NULL,
    NAME TEXT NOT NULL,
    DIVISION TEXT NOT NULL,
    STARS INT NOT NULL) ''')
print("Table created successfully")


def insert_record(id, name, division, stars):
    dbase.execute(''' 
            INSERT INTO employee_records(ID,NAME,DIVISION,STARS)
            VALUES(?,?,?,?)''', (id, name, division, stars))
    print("Record inserted")


# We call the function as many times as we need to insert records
#insert_record(8, 'Romain', 'Hardware', 5)
#insert_record(9, 'Elise', 'HR', 3)

# With Pandas Lib, how to fetch and display records
# from the employee_records table created in the above example.
def read_data():
    query = '''SELECT * FROM employee_records ORDER BY NAME'''
    results = pd.read_sql_query(query, dbase)
    print(results)


def read_data_filter(id):
    query = 'SELECT * FROM employee_records WHERE ID=' + str(id) + ' ORDER BY NAME '
    results = pd.read_sql_query(query, dbase)
    print()
    print(results)


read_data()
read_data_filter(9)

dbase.close()
print('Database Closed')