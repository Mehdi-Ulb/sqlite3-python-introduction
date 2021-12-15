import sqlite3
import requests
import pandas as pd
import datetime as dt


dbase = sqlite3.connect('group42_project.db', isolation_level=None) #open db
dbase.execute("PRAGMA foreign_keys = 1") #pour s'assurer que les FK qu'on crée soient les PK d'autres tables

#####################################
# READ A SPECIFIC TABLE WITH PANDAS #
#####################################
def read_data_pandas(table):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    dbase.execute("PRAGMA foreign_keys = 1")
    
    query = '''SELECT * FROM ''' + str(table)
    results = pd.read_sql_query(query, dbase)
    print(results)
    
    dbase.close()
    return results
#read_data_pandas("invoice")
# Tables = ("Customer", "Company", "Subscription", "Quote", "Invoice", "Payment")
# for i in Tables:
#     print(read_data_pandas(i))


###############################
# READ ALL TABLES WITH PANDAS #
###############################
def read_all_tables_pandas():
    Tables = ("Customer", "Company", "Subscription", "Quote", "Invoice", "Payment")
    for i in Tables:
        print("Table name: " + str(i))
        print(read_data_pandas(i))
        print("\n")
# print(read_all_tables_pandas)


##################################################################################################################
# REQUEST IF CUSTOMER HAS AN INVOICE (OK TO ASSUME #INVOICE AND #QUOTE HAS THE SAME NUMBER BECAUSE RELATION 1-1) #
##################################################################################################################
def request_invoice(email):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    
    #On récupère toutes les invoices de ce client
    Customer_invoices = dbase.execute('''SELECT * FROM Invoice WHERE Customer_Email = ? ''', (email,)).fetchall()
    
    dbase.close()

    #Servira à vérifier si invoice date < aujourd'hui
    current_date = str(dt.datetime.now()).split(" ")[0]
    today = int(current_date.replace("-", ""))
    
    invoice_pending = {} #On y ajoutera les invoices au format "invoice_id" : "invoice_price" due on "invoice_date"
    invoice_paid = [] #On y ajoutera les id des invoices déjà payées
    
    for invoice in Customer_invoices:
        
        invoice_date = int(invoice[3].replace("-", ""))
        
        if invoice_date <= int(today):
    
            #On récupère les infos de l'invoice : id, price, date
            invoice_pending_id = str(invoice[0])
            invoice_pending_price = str(invoice[6]) + "€"
            invoice_date = str(invoice[3])
            
            #On ajoute au dictionnaire invoice_pending les keys "invoice_pending_id" (= id) et les values "invoice_price" due on "invoice_date"
            invoice_pending[invoice_pending_id] = invoice_pending_price + " due on " + invoice_date
            
        else:
            invoice_paid_id = str(invoice[0])
            invoice_paid.append(invoice_paid_id) #On ajoute les id des invoices déjà payées à la liste invoice_paid
            
    print("You have already paid invoice(s) number " + str(invoice_paid) + " and you still need to pay these invoices (id:price & date): " + str(invoice_pending)) #S'affiche dans le terminal
    return "You have already paid invoice(s) number " + str(invoice_paid) + " and you still need to pay these invoices (id:price & date): " + str(invoice_pending) #Ne s'affiche pas dans le terminal
#request_invoice("mehdi.karkour@ulb.be")


#########################################
# PAY INVOICE (aka ADD DATA TO PAYMENT) #
#########################################
def Pay_Invoice(Invoice_Reference, Customer_Email, CC, Amount_Paid_EUR, Currency):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    dbase.execute("PRAGMA foreign_keys = 1")
    
    #On récupère la date de facturation
    Current_Invoice_Date = dbase.execute(''' SELECT Invoice_Date From Invoice WHERE Customer_Email = ?''', (str(Customer_Email),)).fetchall()[0][0]
    
    #On convertie le montant "Amount_Paid_EUR" par celui dans la Currency choisie
    Amount_Paid_FOREIGN = change_eur_to_foreign(float(Amount_Paid_EUR), Currency)
    
    
    dbase.execute(''' INSERT INTO Payment(Invoice_Reference, Current_Invoice_Date, Customer_Email, CC, Amount_Paid_EUR, Amount_Paid_FOREIGN, Currency)
            VALUES(?,?,?,?,?,?,?)
                ''', (Invoice_Reference, Current_Invoice_Date, Customer_Email, CC, Amount_Paid_EUR, Amount_Paid_FOREIGN, Currency)
                  )
    
    dbase.close()

    print(str(Customer_Email) + " has paid " + str(Amount_Paid_FOREIGN) + str(Currency) + " (" + str(Amount_Paid_EUR) + "€) for invoice #" + str(Invoice_Reference) + " due on " + str(Current_Invoice_Date))
    return str(Customer_Email) + " has paid " + str(Amount_Paid_FOREIGN) + str(Currency) + " (" + str(Amount_Paid_EUR) + "€) for invoice #" + str(Invoice_Reference) + " due on " + str(Current_Invoice_Date)
#Pay_Invoice("2", "noeline.deterwangne@ulb.be", "5301150401171978", "24.151600000000002", "EUR")


###############################################################################
# ONCE THE MONTHLY INVOICE HAS BEEN PAID, WE INCREASE INVOICE_DATE BY 1 MONTH #
###############################################################################
def Invoice_paid(invoice_id):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    dbase.execute("PRAGMA foreign_keys = 1")
    
    Current_Invoice_Date = dbase.execute('''SELECT Invoice_Date FROM Invoice WHERE Invoice_ID = {id}'''.format(id = invoice_id)).fetchall()[0][0]
    Invoice_Date = Current_Invoice_Date("-")
    
    #On sépare les jours, mois et années pour les manipuler plus facilement
    current_year = int(Invoice_Date[0])
    current_month = int(Invoice_Date[1])
    current_day = int(Invoice_Date[2])
    
    if current_month == 12: #Si on est au 12e mois de l'année
        new_year = current_year + 1
        new_month = "01"
        
    else: #Si on est à un autre mois que le 12e
        new_year = current_year
        
        if current_month >= 9: #Si le mois est 9, 10 ou 11
            new_month = current_month + 1
        else: #Si le mois est entre 1 et 8, on remet un 0 devant pour garder le bon format
            new_month = "0" + str(current_month + 1)
            
    #On remet la date séparée au bon format (YYYY-MM-DD)
    next_billing_date = str(new_year) + "-" + str(new_month) + "-" + str(current_day)
    
    print("Next billing date: " + str(next_billing_date)) #S'affiche dans le terminal
    
    
    #On met à jour la date de facturation avec la nouvelle date (qui est celle augmentée d'un mois)
    dbase.execute('UPDATE Invoice SET Invoice_Date=? WHERE Invoice_ID = ?',(str(next_billing_date), invoice_id))
    
    dbase.close()
    
    print("Invoice number " + str(invoice_id) + " has been paid, the next billing is due on" + str(next_billing_date)) #S'affiche dans le terminal
#Invoice_paid("5")


##############################
# CREDIT CARD VALIDITY CHECK #
##############################
def CC_check(CC):
    string_CC = str(CC) #Pour pouvoir le parcourir dans une boucle for

    #On va enregistrer la valeur du dernier chiffre dans last_digit et ensuite le supprimer de la liste list
    list = []
    for number in string_CC:
        list.append(int(number))
    last_digit = list[15]
    del list[15]

    #On va retourner la suite de chiffres
    reverse_CC = []
    i = 14
    while i >= 0:
        last = list[i]
        reverse_CC.append(last)
        i = i - 1

    #On double les chiffres en position pair (position 0, position 2, position 4, ...)
    new_reverse = []
    j = 0
    while j <= 14:
        if j % 2 == 0: #Si chiffre pair (aka le reste de la division par deux est 0)
            double = 2 * reverse_CC[j]
            
            if double > 9: #Si quand on double le chiffre (en position pair) il est supérieur à 9, on doit retrancher 9 à ce nombre
                double = double - 9
                new_reverse.append(double)
                
            else:
                new_reverse.append(double)
                
        else:
            simple = reverse_CC[j]
            new_reverse.append(simple)
        j = j + 1

    #4 On additionne tous les chiffres (les 15) ainsi que le dernier chiffre qu'on avait supprimé temporairement (le 16e)
    new_CC = 0
    
    for k in new_reverse:
        new_CC = new_CC + int(k)
        
    new_CC = new_CC + last_digit
    
    if int(new_CC) % 10 == 0: #On vérifie si le résultat de cette somme est divisible par 10
        print("Yes it is, your card number is valid") #S'affiche dans le terminal
        return True
    else:
        print("No it's not, your card number is not valid. Please try again") #S'affiche dans le terminal
        return False
#CC_check(5301150401171978) #Ce numéro est valide
#CC_check(1234123412341234) #Ce numéro n'est pas valide
    
    
#############################################
# CHANGE PRICE IN EUROS TO ANOTHER CURRENCY #
#############################################
def change_eur_to_foreign(price, currency):
    online_currency = requests.get("https://v6.exchangerate-api.com/v6/06099ddd07168bc8d7db68eb/latest/{}".format(currency)).json() #Dictionnaire dans un dictionnaire
    
    exchange_rate = online_currency["conversion_rates"]["EUR"] #conversion_rates est un dictionnaire (qui est lui-même dans un dictionnaire)
    
    price_in_foreign = price / exchange_rate # EUR * FOREIGN/EUR
    
    print("Daily rate: " + str(exchange_rate) + " " + str(currency)) + "/EUR"
    print(str(price) + "€ = " + str(price_in_foreign) + " " + str(currency))
    return price_in_foreign
#change_currency(1000, "EUR")


###########
# ADD VAT #
###########
def add_VAT(price_without_VAT, VAT):

    price_with_VAT = price_without_VAT * (1 + VAT)
    
    print("Total price excluding VAT: " + str(price_without_VAT) + " EUR") #S'affiche dans le terminal
    print("Total price including VAT: " + str(price_with_VAT) + " EUR") #S'affiche dans le terminal
    
    return price_with_VAT
#add_VAT(1000, 0.21)



##################################
# READ ANALYTICS (FOR COMPANIES) #
##################################

#1 Monthly Recurring Revenue (MRR)
def MRR(company_VAT_Number,date_choose):
    date_choose_split=date_choose.split("-")
    year_choose=date_choose_split[0]
    month_choose=date_choose_split[1]
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    company_name = dbase.execute('''SELECT Company_Name FROM Company WHERE VAT_Number= ? ''',(company_VAT_Number,)).fetchall()[0][0]
    quote_invoice = dbase.execute('''SELECT Start_Date, Invoice_ID, Quote_ID, Company_VAT_Number, Price_With_VAT, Quantity
    FROM Quote 
    INNER JOIN Invoice ON Quote_ID = Invoice_ID
    WHERE Company_VAT_Number="{}"  AND Active = True'''.format(company_VAT_Number)).fetchall()

    print(quote_invoice)
    good_month=[]
    for element in quote_invoice:
        for e in element:
            date=str(e)
            break
        date_split=date.split("-")
        year=date_split[0]
        month=date_split[1]
        if month==month_choose:
            if year==year_choose:
                good_month.append(element)
    
    revenue = 0
    for sub in good_month:
        revenue+=sub[4]
    print("The monthly recurring revenue of {} ".format(company_name) +   "is "  +  str(revenue)  +  "EUR (per month)")

    dbase.close()
    return "The monthly recurring revenue of {} ".format(company_name) +  "is "  +  str(revenue)  +  "EUR (per month)"

#MRR("BE444444444","2021-12-15")


#2 Annual Recurring Revenue (ARR)
def ARR(company_VAT_Number,date_choose):
    date_choose_split=date_choose.split("-")
    year_choose=date_choose_split[0]
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    company_name = dbase.execute('''SELECT Company_Name FROM Company WHERE VAT_Number= ? ''',(company_VAT_Number,)).fetchall()[0][0]
    quote_invoice = dbase.execute('''
    SELECT Start_Date, Price_With_VAT as Amount, subscription_name as Subscription
    FROM Quote
    INNER JOIN Invoice ON Quote_ID = Invoice_ID 
    LEFT JOIN Subscription on subscription_id=subscription_reference
    WHERE Quote.Company_VAT_Number="{}"  AND Active = True
    '''.format(company_VAT_Number)).fetchall()

    good_year=[]
    for element in quote_invoice:
        for e in element:
            date=str(e)
            break
        date_split=date.split("-")
        year=date_split[0]
        if year==year_choose:
            good_year.append(element)
    
    revenue = 0
    for sub in good_year:
        revenue+=sub[1]

    print("The annual recurring revenue of {}".format(company_name) + "is " + str(revenue) + "EUR (per year)")

    dbase.close()
    return "The annual recurring revenue of {} ".format(company_name) + "is "  +  str(revenue) + "EUR (per year)"

#ARR("BE444444444","2021-12-15")

def average_revenue(company_VAT_number):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    quote_invoice = dbase.execute('''
    SELECT customer_email As customer, total_amount, company_name
    FROM Quote
    INNER JOIN Invoice ON Quote_ID = Invoice_ID
    LEFT JOIN Company ON company_vat_number=vat_number
    WHERE Quote.Company_VAT_Number="{}"  AND Active = True
    '''.format(company_VAT_number)).fetchall()
    
    for quote in quote_invoice:
        company_name = quote[2]
        break

    total_quote=len(quote_invoice)

    revenue_all=0
    for amount in quote_invoice:
        revenue_all+=amount[1]

    number_customer =[]
    for email in quote_invoice:
        number_customer.append(email[0])
    total_customer = len(number_customer) 

    average_revenue = revenue_all/total_quote
    average_revenue_customer = average_revenue/total_customer

    dbase.close()
    print("The average revenur per customers for all {} quotes is ".format(company_name) + str(average_revenue_customer) + " EUR")
    return "The average revenur per customers for all {} quotes is ".format(company_name) + str(average_revenue_customer) + " EUR"

#average_revenue("BE444444444")


# à supprimer 
# def MRR(date): #format: YYYY-MM-DD
#     date_int = date.replace("-", "") #new format : YYYYMMDD
#     All_invoices = dbase.execute('''SELECT * FROM Invoice''').fetchall() #on récupère toutes les invoices
#     print("Invoices for this month: " + str(All_invoices))
#     Monthly_revenue = 0
#     Quotes_concerned = [] #on y ajoutera les quotes liées aux invoices qui sont dans l'intervalle de date
#     for x in All_invoices:
#         start_date = x[1]
#         start_date_int = start_date.replace("-", "")
#         end_date = x[2]
#         end_date_int = end_date.replace("-", "")
#         if int(start_date_int) <= int(date_int) <= int(end_date_int): #on regarde si la date donnée est entre start_date et end_date
#             print(str(date_int) + " is between " + str(start_date_int) + " and " + str(end_date_int))
#             Quote_linked = dbase.execute(''' SELECT * FROM Quote WHERE Quote.Quote_ID = {id}'''.format(id = str(x[4]))).fetchall() #on récupère la quote de liée à l'invoice actuelle
#             Quotes_concerned.append(Quote_linked)
#             print('We add this quote to the list "Quote_concerned": ' + str(Quote_linked))
#     print("Quote concerned:" + str(Quotes_concerned))
#     for single_quote in Quotes_concerned: #On parcourt les quotes de la liste qu'on vient de créer pour en extraire le prix
#         for price in single_quote:
#             Monthly_revenue = Monthly_revenue + price[3] #Pour avoir le total des revenus de chaque quote
            
#     return print("MRR is for month: " + str(Monthly_revenue) + "€")
    
# MRR("2021-12-15")

#print(str(read_data_fetch()))
    
#print(str(read_data_fetch))

############################################
# Retrieve number of clients for a company #
############################################

def number_client(Company_VAT_Number):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    company_name = dbase.execute('''SELECT Company_Name FROM Company WHERE VAT_Number= ? ''',(Company_VAT_Number,)).fetchall()[0][0]
    print("Company name: " + str(company_name))
    
    quote = dbase.execute('''SELECT * FROM Quote WHERE Company_VAT_Number = ? AND Active=TRUE''', (Company_VAT_Number,)).fetchall()
    number_quote = len(quote)
    
    customer_in_compagny = []
    for element in range(number_quote):
        id_quote = quote[element][0]
        customer_in_compagny.append(id_quote)
    print("All quotes" + str(quote) , "And more precisely, the quote id :"+ str(customer_in_compagny))
    
    customer_email=[]
    for id in customer_in_compagny:
        quote_to_invoice = dbase.execute('''SELECT Customer_Email FROM Invoice WHERE Quote_Reference= ?''',(id,)).fetchall()[0][0]
        customer_email.append(quote_to_invoice)
    
    for email in customer_email:
        twice=customer_email.count(email)
        while twice>1:
            customer_email.remove(email)
            twice-=1

    number_client=len(customer_email)
    # print(customer_email)

    subscription_id=[]
    subscription_product=[]
    for q in quote:
        subscription_id.append(q[2])
    for s in subscription_id:
        subscription_name=dbase.execute('''SELECT Subscription_Name FROM Subscription WHERE Subscription_ID=?''',(s,)).fetchall()[0][0]
        subscription_product.append(subscription_name)
    
    for subscription in subscription_product:
        twice=subscription_product.count(subscription)
        while twice >1:
            subscription_product.remove(subscription)
            twice-=1
    # print(subscription_id)
    # print(subscription_name)
    #print(subscription_product)
    dbase.close()

    print("The company {} have".format(company_name), number_client,"Customer in total, for the following subscription : ",subscription_product)
    return "The company {} have ".format(company_name) + str(number_client) + " Customer in total, for the following subscription : "+ str(subscription_product)

# number_client("BE444444444")
# VAT = dbase.execute('''SELECT VAT_Number FROM Company''').fetchall()
# for one_vat in VAT:
#     number_client(one_vat[0])


def customer_subscription(VAT):
    dbase = sqlite3.connect('group42_project.db', isolation_level=None)
    dbase.execute("PRAGMA foreign_keys = 1")

    query = '''SELECT Company_Name AS Company, Subscription_Name AS Subscription,Customer_Email AS Customer, Total_Amount AS Amount
    FROM Quote 
    INNER JOIN Invoice ON Invoice_ID = Quote_ID 
    INNER JOIN Subscription ON Subscription_Reference = Subscription_ID
    LEFT JOIN Company ON Quote.Company_VAT_Number = VAT_Number
    WHERE Quote.Company_VAT_Number="{}"  
    ORDER BY Customer_Email  '''.format(VAT)
    

    results = pd.read_sql_query(query,dbase)


    dbase.close()
    print(results)
    return results

#customer_subscription("BE444444444")

dbase.close()
print("Database closed")