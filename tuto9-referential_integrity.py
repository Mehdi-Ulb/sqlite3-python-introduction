import sqlite3
dbase = sqlite3.connect('project_database.db', isolation_level=None)
# Enable Referential Integrity Check. (For the project)
dbase.execute("PRAGMA foreign_keys = 1")

print('Database opened')

# Companies
dbase.execute(''' 
        CREATE TABLE IF NOT EXISTS Companies 
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            name TEXT NOT NULL,
            info TEXT
        ) 
        ''')
print("Table Companies created successfully")

# Contracts
dbase.execute(''' 
        CREATE TABLE IF NOT EXISTS Contracts 
        (
            id_contract INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
            companies_id INTEGER NOT NULL,
            signed_off DATE NOT NULL,
            FOREIGN KEY (companies_id) REFERENCES Companies(id) 
        )''')
print("Table Companies created successfully")

dbase.execute(''' 
                INSERT INTO Companies
                (name, info)
                VALUES ('TESLA', 'My Description')
            ''')


def insert_contracts(companies_id, signed_off):
    dbase.execute('''
    
            INSERT INTO Contracts(
                companies_id,signed_off)
            VALUES(?,?)
                ''', (companies_id, signed_off)

                  )
    print("Record inserted")


insert_contracts(1, '2021-11-09')
# Test - referential integrity
insert_contracts(1800, '2021-11-10')


dbase.close()
print('Database Closed')