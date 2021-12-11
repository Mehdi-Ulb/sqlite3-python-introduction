# Please use these links and not this code
#
# Code :    https://github.com/chriscastan/digital_firm_tp-10
# Theory :  https://doc.clickup.com/p/h/2fc07-21321/45536472e940cce/2fc07-21381


# Not included in the assessment scheduled for 23 November 2021
import sqlite3
import uvicorn
from fastapi import FastAPI
from starlette.requests import Request

app = FastAPI()


# Router
@app.get("/")
def root():
    return {"message": "It works !"}



@app.get("/contracts")
def read_users():
    dbase = sqlite3.connect('project_database.db', isolation_level=None, check_same_thread=False)
    dbase.execute("PRAGMA foreign_keys = 1")
    b = dbase.execute('''SELECT * FROM contracts''').fetchall()
    dbase.close()
    print('Database Closed')
    return b



if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
    # uvicorn main:app --port 8000 --reload
    # open your web browser and : localhost:8080

# ---------------------------------------------------------------------------------------------------------------------


# Companies
dbase.execute(''' 
        CREATE TABLE IF NOT EXISTS Companies 
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                VALUES ('Microsoft', 'My Description')
            ''')


def insert_contracts(companies_id, signed_off):
    dbase.execute('''

            INSERT INTO Contracts(
                companies_id,signed_off)
            VALUES(?,?)
                ''', (companies_id, signed_off)

                  )
    print("Record inserted")
#insert_contracts(1, '2021-11-09')
# Test - referential integrity
#insert_contracts(2, '2021-11-10')

#dbase.execute('''DROP TABLE Companies''')