# First, we need to import the sqlite3 library in order to interact with the SQLite Database.
import sqlite3

# We open a connection to the SQLite database file.
# If the given database name does not exist then this call will create the database.
# isolation_level=None -> Auto-commit (leave it like this ;-) )
dbase = sqlite3.connect('project_database.db', isolation_level=None)
print('Database opened')

dbase.execute('''DROP TABLE employee_records''')

# 1. FIRST STEP - CREATE THE TABLE WITH "CREATE TABLE"
# When the above program is executed,
# it will create the employee_records table in your project_database.db
dbase.execute(''' 
            CREATE TABLE IF NOT EXISTS employee_records(
            employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
            firstname TEXT NOT NULL,
            name TEXT NOT NULL) 
            ''')
print("Table created successfully")


# 2. SECOND STEP - INSERT VALUES IN THE TABLE WITH "INSERT INTO"
def insert_record(firstname, name):
    dbase.execute(
                    ''' 
                    INSERT INTO employee_records(firstname,name)
                    VALUES(?,?)
                    ''', (firstname, name))


    # print name and firstname in the terminal.
    # Try to print employee_id. What do you have to do ?
    print("Record inserted: " + str(name) + "_" + str(firstname))
    # Record inserted: Georges_Clooney
    # Record inserted: Brad_Pitt

    # As a reminder :
    # str() is to convert a data into a string.
    # int() is to convert a data into a integer


# We call the function as many times as we need to insert records
# Here, 2 times.
insert_record('Georges', 'Clooney')
# Record inserted: Georges_Clooney
insert_record('Brad', 'Pitt')
# Record inserted: Brad_Pitt


# 3. THIRD STEP - READ VALUES WITH "SELECT"
# With fetchall().

def read_data_fetch():
    # we execute an SQL query and information will be stored in the data variable.
    data = dbase.execute(''' SELECT * FROM employee_records ORDER BY employee_id ASC ''').fetchall()
    return data


# Now, we have to use the read_data_fetch() function.
print("With the read_data_fetch function and for")
# read_data_fetch() returns data. And the variable "records" contains the "return data"
records = read_data_fetch()

print("We have a list [] of 2 tuples () : " + str(records))
# [
#   (1, 'Georges', 'Clooney'),
#   (2, 'Brad', 'Pitt')
# ]

print("Do you want to print the first tuple ?")
# We must start with the number 0, because a list starts at 0 and not 1.
print(str(records[0]))
# (1, 'Georges', 'Clooney')

# Now, we want to access to the second element of the first tuple () ?
# records[0] = the first tuple
# records[0][1] = the first tuple and the firstname.
#   employee_id = 0
#   firstname = 1
#   name = 2
# it's like a matrix. First record (0), second element of the record (1).
print(str(records[0][1]))

# As an exercise, test with the second tuple, and the "name" column.
# Answer is : print(str(records[1][2]))


# Now, we will go through each tuple of the list.
# List? -> Use a "for" loop :)
for line in records:
    # line is records[0] then records[1]... until the end of the list.
    print("------------------")
    print("The current tuple is: " + str(line))
    print(str(line[0]) + " is the employee_id")
    print(str(line[1]) + " is the firstname")
    print(str(line[2]) + " is the name")

# ------------------
# The current tuple is: (1, 'Georges', 'Clooney')
# 1 is the employee_id
# Georges is the firstname
# Clooney is the name
# ------------------
# The current tuple is: (2, 'Brad', 'Pitt')
# 2 is the employee_id
# Brad is the firstname
# Pitt is the name

# Don't forget to close the connection between your python code and the SQLite database.
dbase.close()
print('Database Closed')