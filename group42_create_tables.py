import sqlite3
import pandas as pd


dbase = sqlite3.connect('group42_project.db', isolation_level=None)
print('Database opened')
# Enable Referential Integrity Check. (Mandatory for the project)
dbase.execute("PRAGMA foreign_keys = 1")
# By telling SQLite to check for foreign keys, you are enforcing referential integrity
# This means that ensuring that the relationships between tables are appropriate).
# So, if a value appears as a foreign key in one table,
# it must also appear as the primary key of a record in the referenced table.

#1 Customer -- DATE FORMAT : YYYY-MM-DD
dbase.execute(''' CREATE TABLE IF NOT EXISTS Customer (
    Email TEXT PRIMARY KEY NOT NULL,
    First_Name TEXT NOT NULL,
    Last_Name TEXT NOT NULL,
    Birth_Date DATE NOT NULL,
    Password TEXT NOT NULL,
    Address TEXT NOT NULL,
    Billing_Address TEXT NOT NULL,
    Credit_Card INTEGER NOT NULL
    
    ) ''')
print("Customer table created successfully")


#2 Company
dbase.execute(''' CREATE TABLE IF NOT EXISTS Company (
    VAT_Number TEXT PRIMARY KEY NOT NULL,
    Company_Name TEXT NOT NULL,
    Address TEXT NOT NULL,
    Bank_Account TEXT NOT NULL
    
    ) ''')
print("Company table created successfully")



#3 Subscription
dbase.execute(''' CREATE TABLE IF NOT EXISTS Subscription (
    Subscription_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Subscription_Name TEXT NOT NULL,
    Single_Price FLOAT NOT NULL,
    Currency TEXT NOT NULL,
    Company_VAT_Number TEXT NOT NULL,
    FOREIGN KEY(Company_VAT_Number) REFERENCES Company(VAT_Number)
    
    ) ''')
print("Subscription table created successfully")

#4 Quote
dbase.execute(''' CREATE TABLE IF NOT EXISTS Quote (
    Quote_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Company_VAT_Number TEXT NOT NULL,
    Subscription_Reference INTEGER NOT NULL,
    Quantity INTEGER NOT NULL,
    Price_Without_VAT FLOAT NOT NULL,
    Price_With_VAT FLOAT NOT NULL,
    Currency TEXT NOT NULL,
    Active BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (Company_VAT_Number) REFERENCES Company(VAT_Number),
    FOREIGN KEY (Subscription_Reference) REFERENCES Subscription(Subscription_ID)
    
    ) ''')
print("Quote table created successfully")

#5 Invoice
dbase.execute(''' CREATE TABLE IF NOT EXISTS Invoice (
    Invoice_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Quote_Reference INTEGER NOT NULL,
    Start_Date DATE NOT NULL,
    Invoice_Date DATE NOT NULL,
    End_Date DATE NOT NULL,
    Customer_Email TEXT NOT NULL,
    Total_Amount FLOAT NOT NULL,
    Currency TEXT NOT NULL,
    FOREIGN KEY(Customer_Email) REFERENCES Customer(Email),
    FOREIGN KEY(Quote_Reference) REFERENCES Quote(Quote_ID)
    
    ) ''')
print("Invoice table created successfully")

#6 Payment
dbase.execute(''' CREATE TABLE IF NOT EXISTS Payment (
    Payment_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    Invoice_Reference INT NOT NULL,
    Current_Invoice_Date DATE NOT NULL,
    Customer_Email TEXT NOT NULL,
    CC INTEGER NOT NULL,
    Amount_Paid_EUR FLOAT NOT NULL,
    Amount_Paid_FOREIGN FLOAT NOT NULL,
    Currency TEXT NOT NULL,
    FOREIGN KEY(Invoice_Reference) REFERENCES Invoice(Invoice_ID),
    FOREIGN KEY(Customer_Email) REFERENCES Customer(Email)
    
    ) ''')
print("Payment table created successfully")

dbase.close()
print("Database closed")