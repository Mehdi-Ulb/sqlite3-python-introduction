import sqlite3
from pprint import pprint

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

# How to fetch and display records from the employee_records table created in the above example.
def read_data():
    data = dbase.execute(''' SELECT * FROM employee_records ORDER BY NAME''')
    pprint(data)
    for record in data:
        print('ID : '+str(record[0]))
        print('NAME : '+str(record[1]))
        print('DIVISION : '+str(record[2]))
        print('STARS : '+str(record[3])+'\n')


read_data()

dbase.close()
print('Database Closed')