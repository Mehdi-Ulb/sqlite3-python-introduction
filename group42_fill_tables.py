import sqlite3
import requests
import pandas as pd
import group42_project
import datetime as dt

dbase = sqlite3.connect('group42_project.db', isolation_level=None)
print('Database opened')

# Enable Referential Integrity Check. (Mandatory for the project)#
dbase.execute("PRAGMA foreign_keys = 1")
# By telling SQLite to check for foreign keys, you are enforcing referential integrity
# This means that ensuring that the relationships between tables are appropriate).
# So, if a value appears as a foreign key in one table,
# it must also appear as the primary key of a record in the referenced table.


#########################
#1 ADD DATA TO CUSTOMER #
#########################
def insert_Customer(Email, First_Name, Last_Name, Birth_Date, Password, Address, Billing_Address, Credit_Card):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    print('Database opened')
    dbase.execute("PRAGMA foreign_keys = 1")
    
    dbase.execute('''INSERT INTO Customer(Email, First_Name, Last_Name, Birth_Date, Password, Address, Billing_Address, Credit_Card)
            VALUES(?,?,?,?,?,?,?,?)
                ''', (Email, First_Name, Last_Name, Birth_Date, Password, Address, Billing_Address, Credit_Card)
                  )
    print(str(Email) + " added to the database")
    
    dbase.close()
    print('Database closed')
    return True


########################
#2 ADD DATA TO COMPANY #
########################
def insert_Company(VAT_Number, Company_Name, Address, Bank_Account):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    print('Database opened')
    dbase.execute("PRAGMA foreign_keys = 1")
    
    dbase.execute('''INSERT INTO Company(VAT_Number, Company_Name, Address, Bank_Account)
            VALUES(?,?,?,?)
                ''', (VAT_Number, Company_Name, Address, Bank_Account)
                  )   
    print(str(Company_Name) + " added to the database")
    
    dbase.close()
    print('Database closed')
    return True


#############################    
#3 ADD DATA TO SUBSCRIPTION #
#############################
def insert_Subscription(Subscription_Name, Single_Price, Currency, Company_VAT_Number):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    print('Database opened')
    dbase.execute("PRAGMA foreign_keys = 1")
    
    dbase.execute('''INSERT INTO Subscription(Subscription_Name, Single_Price, Currency, Company_VAT_Number)
            VALUES(?,?,?,?)
                ''', (Subscription_Name, Single_Price, Currency, Company_VAT_Number)
                  )
    print(str(Subscription_Name) + " from " + str(Company_VAT_Number) + " added")
    
    dbase.close()
    print('Database closed')
    return True      


######################
#4 ADD DATA TO QUOTE #
######################
def insert_Quote(Company_VAT_Number, Subscription_Reference, Quantity, Currency, Active):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    print('Database opened')
    dbase.execute("PRAGMA foreign_keys = 1")
    
    Single_Price = dbase.execute(''' SELECT Single_Price FROM Subscription WHERE Subscription_ID = ? ''', (Subscription_Reference,)).fetchall()[0][0]
    Price_Without_VAT = Single_Price * int(Quantity)
    print("Price without VAT: " + str(Price_Without_VAT) + "€")
    Price_With_VAT = group42_project.add_VAT(Price_Without_VAT, 0.21)
    print("Price with VAT: " + str(Price_With_VAT) + "€")

    
    dbase.execute('''INSERT INTO Quote(Company_VAT_Number, Subscription_Reference, Quantity, Price_Without_VAT, Price_With_VAT, Currency, Active)
            VALUES(?,?,?,?,?,?,?)
                ''', (Company_VAT_Number, Subscription_Reference, Quantity, Price_Without_VAT, Price_With_VAT, Currency, Active)
                  )
    print("Quote by " + str(Company_VAT_Number) + "succesfully created")
    
    dbase.close()
    print('Database closed')
    return True


########################
#5 ADD DATA TO INVOICE #
########################
def insert_Invoice(Quote_Reference, Customer_Email, End_Date, Currency):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    dbase.execute("PRAGMA foreign_keys = 1")

    
    Total_Amount = dbase.execute('''SELECT Price_With_VAT FROM Quote WHERE Quote_ID =?''', (Quote_Reference,)).fetchall()[0][0]
    
    Start_Date = str(dt.datetime.now()).split(" ")[0]
    
    date_in_list = Start_Date.split("-")
    current_year = int(date_in_list[0])
    current_month = int(date_in_list[1])

    if current_month < 12:
        next_month = current_month + 1
        date_in_list[1] = str(next_month)
    else:
        next_year = current_year + 1
        next_month = "01"
        date_in_list[0] = str(next_year)
        date_in_list[1] = str(next_month)
        
    Invoice_Date = str(date_in_list[0]) + "-" + str(date_in_list[1]) + "-" + str(date_in_list[2])
    
        
    dbase.execute('''INSERT INTO Invoice(Quote_Reference, Customer_Email, Start_Date, Invoice_Date, End_Date, Total_Amount, Currency)
            VALUES(?,?,?,?,?,?,?)
                ''', (Quote_Reference, Customer_Email, Start_Date, Invoice_Date, End_Date, Total_Amount, Currency)
                  )
    print("Invoice for " + str(Customer_Email) + " added to the database for a monthly billing of " + str(Total_Amount) + " " + str(Currency))
    
    dbase.close()
    return True


########################
#6 ADD DATA TO PAYMENT #
########################
# def insert_Payment(Invoice_Reference, Customer_Email, Amount_Paid, Currency):
#     dbase = sqlite3.connect('group42_project.db', isolation_level=None)
#     dbase.execute("PRAGMA foreign_keys = 1")
    
#     CC = dbase.execute('''SELECT Credit_Card From Customer WHERE Email = ?''', (Customer_Email,)).fetchall()[0][0]
    
    
#     Current_Invoice_Date = dbase.execute(''' SELECT Invoice_Date From Invoice WHERE Email = ?''', (Customer_Email,)).fetchall[0][0]
    
    
#     dbase.execute('''
    
#         INSERT INTO Payment(
#             Invoice_Reference, Current_Invoice_Date, Customer_Email, CC, Amount_Paid_EUR, Amount_Paid_FOREIGN, Currency)
#         VALUES(?,?,?,?,?,?,?,?)
#             ''', (Invoice_Reference, Current_Invoice_Date, Customer_Email, CC, Amount_Paid_EUR, Amount_Paid_FOREIGN, Currency)
                
#                )
        
#     print(str(Customer_Email) + " has paid " + str(Amount_Paid) + " for Invoice number " + str(Invoice_Reference)) + " due on" + str(Current_Invoice_Date)

#     dbase.close()

#     return True


# insert_Customer("mehdi.karkour@ulb.be", "Mehdi", "Karkour", "1998-04-21", "Mehdi.123", "Alchemiststeeg 11, 1120 Neder-over-Heembeek, BE", "Rue De Wand 11, 1020 Laeken, BE", "5115800900636253")
# insert_Customer("noeline.deterwangne@ulb.be", "Noëline", "De Terwangne", "1998-02-11", "Noeline.123", "Rue De Wand 12, 1020 Laeken, BE", "Rue De Wand 12, 1020 Laeken, BE", "1234123412341234")
# insert_Customer("yannick.lu@ulb.be", "Yannick", "Lu", "1998-01-01", "Yannick.123", "Avenue Buyl 12, 1000 Ixelles, BE", "Avenue Buyl 12, 1000 Ixelles, BE", "5678567856785678")
# insert_Customer("ilias.mahri@ulb.be", "Ilias", "Mahri", "1998-10-19", "Ilias.123", "Avenue Wannecouter 10, 1020 Laeken, BE", "Avenue Wannecouter 10, 1020 Laeken, BE", "1234123412341234")
# insert_Customer("thien.huynh@ulb.be", "Thien", "Huynh", "1999-05-25", "Thien.123", "Berkendallaan 15, 1800 Vilvoorde, BE", "Berkendallaan 15, 1800 Vilvoorde, BE", "5301150401171978")

# dbase.execute(''' UPDATE Customer SET Credit_Card = "1111111111111111" WHERE Email = "noeline.deterwangne@ulb.be" ''')



# insert_Company("BE111111111", "Spotify", "Alchemiststeeg 159, 1120 Neder-over-Heembeek, BE", "BE00123412341111")
# insert_Company("BE222222222", "Microsoft", "Avenue Buyl 101, 1000 Ixelles, BE", "BE00678967892222")
# insert_Company("BE333333333", "Youtube", "Avenue Wannecouter 13, 1020 Laeken, BE", "BE89123412343333")
# insert_Company("BE444444444", "Netflix", "Rue De Wand 102, 1020 Laeken, BE", "BE00678967894444")
# insert_Company("BE555555555", "Amazon Prime", "Berkendallaan 100, 1800 Vilvoorde, BE", "BE00678967895555")



# insert_Subscription("Spotify Premium", 9.99, "EUR", "BE111111111")                   #1
# insert_Subscription("Spotify Premium Student", 4.99, "EUR", "BE111111111")           #2
# insert_Subscription("Microsoft OneDrive 100 GB", 2.00, "EUR", "BE222222222")         #3
# insert_Subscription("Youtube Premium", 11.99, "EUR", "BE333333333")                  #4
# insert_Subscription("Youtube Premium Student", 6.99, "EUR", "BE333333333")           #5
# insert_Subscription("Amazon Prime", 5.99, "EUR", "BE555555555")                      #6
# insert_Subscription("Netflix Essential (1 screen)", 8.99, "EUR", "BE444444444")      #7
# insert_Subscription("Netflix Standard (2 screen)", 13.49, "EUR", "BE444444444")      #8
# insert_Subscription("Netflix Premium (4 screens)", 17.99, "EUR", "BE444444444")      #9



# insert_Quote("BE111111111", "1", "2", "EUR", True)    #1
# insert_Quote("BE222222222", "2", "4", "EUR", True)    #2
# insert_Quote("BE333333333", "4", "1", "EUR", True)    #3
# insert_Quote("BE444444444", "7", "5", "EUR", True)    #4
# insert_Quote("BE555555555", "6", "1", "EUR", True)    #5
# insert_Quote("BE444444444", "8", "5", "EUR", True)    #6
# insert_Quote("BE444444444", "9", "5", "EUR", True)    #7
# insert_Quote("BE444444444","9", "5", "EUR", True)


# insert_Invoice("1", "mehdi.karkour@ulb.be", "2030-11-24", "EUR")          #1
# insert_Invoice("2", "noeline.deterwangne@ulb.be", "2030-11-25", "EUR")    #2
# insert_Invoice("3", "yannick.lu@ulb.be", "2030-10-24", "EUR")             #3
# insert_Invoice("4", "ilias.mahri@ulb.be", "2030-11-28", "EUR")            #4
# insert_Invoice("5", "thien.huynh@ulb.be", "2030-12-27", "EUR")            #5
# insert_Invoice("6","mehdi.karkour@ulb.be", "2030-11-24", "EUR")           #6
# insert_Invoice("7", "ilias.mahri@ulb.be", "2030-11-28", "EUR")            #7
# insert_Invoice("8","mehdi.karkour@ulb.be","2030-11-23","EUR")


# dbase.execute('UPDATE Invoice SET Invoice_Date="2021-10-24" WHERE Invoice_ID = 1')    #1
# dbase.execute('UPDATE Invoice SET Invoice_Date="2021-10-25" WHERE Invoice_ID = 2')    #2
# dbase.execute('UPDATE Invoice SET Invoice_Date="2021-09-24" WHERE Invoice_ID = 3')    #3


dbase.close()
print("Database closed")