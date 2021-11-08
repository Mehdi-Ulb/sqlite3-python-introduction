import sqlite3

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

dbase.close()
print('Database Closed')