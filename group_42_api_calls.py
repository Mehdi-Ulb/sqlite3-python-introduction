import sqlite3
import pandas as pd
import numpy
import group42_fill_tables
import group42_project
import datetime as dt


# We need to import the Request object as well:
from fastapi import FastAPI, Request
import uvicorn

app = FastAPI()

@app.get("/")
def root():
      return {"message": "It works !"}

#######################
# Register a customer #
#######################
@app.post("/register_customer")
async def register_customer(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      group42_fill_tables.insert_Customer(str(values_dict['Email']),
                                          str(values_dict['First_Name']),
                                          str(values_dict['Last_Name']),
                                          str(values_dict['Birth_Date']),
                                          str(values_dict['Password']),
                                          str(values_dict['Address']),
                                          str(values_dict['Billing_Address']),
                                          str(values_dict['Credit_Card'])
                                          )
      
      return "Customer {email} correctly registered to database".format(email = str(values_dict['Email']))


######################
# Register a company #
######################
@app.post("/register_company")
async def register_company(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      group42_fill_tables.insert_Company(str(values_dict['VAT_Number']),
                                         str(values_dict['Company_Name']),
                                         str(values_dict['Address']),
                                         str(values_dict['Bank_Account'])
                                         )
      
      return "{name} correctly registered to database".format(name = str(values_dict['Company_Name']))


###########################
# Register a subscription #
###########################
@app.post("/register_subscription")
async def register_subscription(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      group42_fill_tables.insert_Subscription(str(values_dict['Subscription_Name']),
                                              str(values_dict['Single_Price']),
                                              str(values_dict['Currency']),
                                              str(values_dict['Company_VAT_Number'])
                                              )
      
      return "{sub_name} from company {vat} correctly added to database".format(sub_name = str(values_dict['Subscription_Name']), vat = str(values_dict['Company_VAT_Number']))
  

####################
# Register a quote #
####################
@app.post("/register_quote")
async def register_quote(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      subscription_id = str(values_dict['Subscription_Reference'])
      
      dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False) #open db
      Company_VAT_Number_In_Subscription = dbase.execute('''SELECT Company_VAT_Number FROM Subscription WHERE Subscription_ID = ?''', (subscription_id )).fetchall()[0][0]
      dbase.close() #close db
      
      
      if str(values_dict['Active']) == "False": #On ne peut pas créer une quote directement avec le statut "Active : TRUE" car elle ne serait liée à aucune invoice
            group42_fill_tables.insert_Quote(str(Company_VAT_Number_In_Subscription),
                                             str(values_dict['Subscription_Reference']),
                                             str(values_dict['Quantity']),
                                             str(values_dict['Currency']),
                                             False
                                             )
            
            return "Subcription number {reference} from {vat} added to database".format(reference = str(values_dict['Subscription_Reference']), vat = str(Company_VAT_Number_In_Subscription))
      else:
            return "You cannot create a quote with an Active status"


##########################
# Display specific quote #
##########################

@app.get("/display_specific_quote")
async def display_specific_quote(payload: Request):
      values_dict = await payload.json()
      dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False)
      
      specific = dbase.execute(''' SELECT Company_VAT_Number, Subscription_Reference, Quantity, Price_Without_VAT, Price_With_VAT, Currency, Active FROM Quote
                               WHERE Quote_ID = {}'''.format(str(values_dict['quote_id']))).fetchall()
      quote = {"Company_VAT_Number" : [], "Subscription_Reference" : [], "Quantity" : [], "Price_Without_VAT" : [], "Price_With_VAT" : [], "Currency" : [], "Active" : []}
      position = 0
      for key in quote:
            for value in specific:
                  quote[key].append(value[position])
                  position = position + 1
      dbase.close()
      return quote


####################################################
#  Accept quote -> automatically create an invoice #
####################################################
@app.post("/accept_quote")
async def accepte_quote(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      
      dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False)
      
      #Servira à vérifier que le statut de la quote n'est pas "Active : TRUE"
      quote_id = str(values_dict['Quote_Reference'])
      quote_statut = dbase.execute(''' SELECT * FROM Quote WHERE Quote_ID = ?''', (quote_id,)).fetchall()[0][7]
      
      dbase.close()
      
      if quote_statut == True:
            return "This quote is already binded to an invoice, please create a new quote"
      
      elif quote_statut == False:
            
            group42_fill_tables.insert_Invoice(str(values_dict['Quote_Reference']),
                                            str(values_dict['Customer_Email']),
                                            str(values_dict['End_Date']),
                                            str(values_dict['Currency'])
                                            )
            
            #We update the "Active" statut of the current quote to TRUE
            dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False)
            
            dbase.execute('''UPDATE Quote SET Active = ? WHERE Quote_ID = ?''', (True, quote_id,))
            
            dbase.close()
            
            return "New quote number {reference} correctly registered for {email}".format(reference = str(values_dict['Quote_Reference']), email = str(values_dict['Customer_Email']))


############################################
#  Check if customer has a pending invoice #
############################################
@app.get("/check_pending_invoice")
async def check_pending_invoice(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      return group42_project.request_invoice(str(values_dict['Email'])) #Permet de retourner le "return" de la fonction request_invoice


#######################################
# PAY INVOICE BY USING CC IN ACCOUNT #
#######################################
@app.post("/pay_invoice")
async def pay_invoice(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False) #open db
      
      #Servira à check si l'invoice existe
      customer_email = str(values_dict['Customer_Email'])
      invoice_id = int(values_dict["Invoice_Reference"])
      check_if_invoice_exists = len(dbase.execute('''SELECT * FROM Invoice WHERE Customer_Email = ? AND Invoice_ID = ?''', (customer_email, invoice_id,)).fetchall())
      
      #On récupère la CC enregistrée dans le compte du client et on prend la date sur l'invoice
      CC_registered = dbase.execute('''SELECT Credit_Card FROM Customer WHERE Email = ?''', (customer_email, )).fetchall()[0][0]
      Invoice_Date = dbase.execute('''SELECT Invoice_Date FROM Invoice WHERE Customer_Email = ?''', (customer_email, )).fetchall()[0][0]
      
      dbase.close() #close db
      
      #Servira à vérifier si la date d'invoice < aujourd'hui (et dans ce cas là on a une invoice à payer)
      invoice_date_number = Invoice_Date.replace("-","")
      today = str(dt.datetime.now()).split(" ")[0]
      today_number = today.replace("-", "")
      
      if check_if_invoice_exists == 1:
            if int(invoice_date_number) < int(today_number):
                  if group42_project.CC_check(CC_registered) == True: #Si la carte est valide
                        
                        group42_project.Pay_Invoice(str(values_dict['Invoice_Reference']),
                                                    str(values_dict['Customer_Email']),
                                                    str(CC_registered),
                                                    str(values_dict['Amount_Paid_EUR']),
                                                    str(values_dict['Currency'])
                                                    )
                        
                        #On augmente le mois de facturation d'un mois vu que le précédent a été payé
                        group42_project.Invoice_paid(str(values_dict['Invoice_Reference']))    
                        
                        return group42_project.Pay_Invoice(str(values_dict['Invoice_Reference']),
                                                           str(values_dict['Customer_Email']),
                                                           str(CC_registered),
                                                           str(values_dict['Amount_Paid_EUR']),
                                                           str(values_dict['Currency'])
                                                           ) #Permet de renvoyer le "return" de la fonction Pay_Invoice
                  else:
                        return "Sorry " + str(values_dict['Customer_Email']) + " but your CC is incorrect, please update it"
            else:
                  return "Hello " + str(values_dict['Customer_Email']) + ", you don't have pending invoice"
      else:
            return "The invoice number is not yours or doesn't exist. If you think this is an error, please contact your administrator."


#############################
# UPDATE CC AND PAY INVOICE #
#############################
@app.post("/update_cc_and_pay_invoice")
async def update_cc_and_pay_invoice(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False) #open db
      
      #Servira à check si l'invoice existe
      customer_email = str(values_dict['Customer_Email'])
      invoice_id = int(values_dict["Invoice_Reference"])
      check_if_invoice_exists = len(dbase.execute('''SELECT * FROM Invoice WHERE Customer_Email = ? AND Invoice_ID = ?''', (customer_email, invoice_id,)).fetchall())
      
      #On récupère la date sur l'invoice
      Invoice_Date = dbase.execute('''SELECT Invoice_Date FROM Invoice WHERE Customer_Email = ?''', (customer_email, )).fetchall()[0][0]
      
      dbase.close() #close db
      
      New_CC = str(values_dict['CC'])
      
      #Servira à vérifier si la date d'invoice < aujourd'hui (et dans ce cas là on a une invoice à payer)
      invoice_date_number = Invoice_Date.replace("-","")
      today = str(dt.datetime.now()).split(" ")[0]
      today_number = today.replace("-", "")
      
      
      if check_if_invoice_exists == 1:
            if int(invoice_date_number) < int(today_number):
                  if group42_project.CC_check(New_CC) == True: #Si la nouvelle carte est valide

                        group42_project.Pay_Invoice(str(values_dict['Invoice_Reference']),
                                                    str(values_dict['Customer_Email']),
                                                    str(values_dict['CC']),
                                                    str(values_dict['Amount_Paid_EUR']),
                                                    str(values_dict['Currency'])
                                                    )
                        
                        #On met à jour la CC présente dans le compte du client avec la nouvelle carte valide
                        dbase = sqlite3.connect('group42_project.db', isolation_level=None, check_same_thread=False)
                        dbase.execute('''UPDATE Customer SET Credit_Card = ? WHERE Email = ?''', (New_CC, str(values_dict['Customer_Email'])))
                        dbase.close()
                        
                        #On augmente le mois de facturation d'un mois vu que le précédent a été payé
                        group42_project.Invoice_paid(str(values_dict['Invoice_Reference']))
                        
                        
                        return group42_project.Pay_Invoice(str(values_dict['Invoice_Reference']),
                                                           str(values_dict['Customer_Email']),
                                                           str(values_dict['CC']),
                                                           str(values_dict['Amount_Paid_EUR']),
                                                           str(values_dict['Currency'])
                                                           ) #Permet de renvoyer le "return" de la fonction Pay_Invoice
                              
                  elif group42_project.CC_check(New_CC) == False: #Si la nouvelle carte n'est pas valide
                        return "Sorry " + str(values_dict['Customer_Email']) + " but your CC is still incorrect, please update it"
            
            else:
                  return "Hello " + str(values_dict['Customer_Email']) + ", you don't have pending invoice"
      else:
            return "The invoice number is not yours or doesn't exist. If you think this is an error, please contact your administrator."
    
  
############################################################################
# Display MRR & ARR and the average revenue per customer for one compagnie #
############################################################################
@app.post("/display_analytics")
async def display_analytics(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      Company_VAT = str(values_dict['Company_VAT_Number'])
      Date = str(values_dict['Choosen_Date'])
      
      MRR = group42_project.MRR(Company_VAT,Date)
      ARR = group42_project.ARR(Company_VAT,Date)
      Average_revenue = group42_project.average_revenue(Company_VAT)
      
      return (MRR,ARR,Average_revenue)




##############################################
# Display number of client for one compagnie #
##############################################
@app.post("/display_customer")
async def display_customer(payload: Request):
      values_dict = await payload.json() #Dictionnaire de pairs
      
      Company_VAT = str(values_dict['Company_VAT_Number'])
      
      clients = group42_project.number_client(Company_VAT)
      
      return clients

##################################################
# Show customer and subscription for one company #
##################################################
@app.post("/display_cust_sub")
async def display_cust_sub(payload: Request):
      values_dict = await payload.json()
      
      Company_VAT = str(values_dict['Company_VAT_Number'])
      
      table = group42_project.customer_subscription(Company_VAT)
      
      return table












if __name__ == '__main__':
      uvicorn.run(app, host='127.0.0.1', port=8000)
  
  

























































  
#Ce qu'il y a ci-dessous ne fait pas partie du code 
  
############################################
# 2. Confirm student attendance to an exam
############################################
@app.post("/confirm_attendance")
async def confirm_attendance(payload: Request):
  values_dict = await payload.json()
  # Open the DB
  dbase = sqlite3.connect('tp10.db', isolation_level=None, check_same_thread=False)

  # Step 1: retrieve the secret and id based on matricule

  secret_query = dbase.execute(''' 
                SELECT id, secret FROM Students
                WHERE id = {}
                '''.format(str(values_dict['student_id'])))

  secret = secret_query.fetchall()[0][1]

  # Step 2: check if secret is effectively equal:
  
  if secret != values_dict['secret']:
    return "error"
  exam = dbase.execute(''' 
                  UPDATE Exams
                  SET attendance = 1
                  WHERE session_id = {session_id}
                  AND student_id = {student_id}        
                  '''.format(session_id = values_dict['session_id'], student_id = values_dict['student_id']))
  # Close the DB
  dbase.close()
  return "Student {student_id} correcty registered to Session {session_id}".format(session_id = values_dict['session_id'], student_id = values_dict['student_id']) 



############################################
# 3. Grade an exam
############################################

@app.post("/grade_exam")
async def grade_exam(payload: Request):
  values_dict = await payload.json()
  # Open the DB
  dbase = sqlite3.connect('tp10.db', isolation_level=None, check_same_thread=False)

  # Step 1: retrieve the secret and id based on matricule

  secret_query = dbase.execute(''' 
                SELECT id, secret FROM Teachers
                WHERE id = {}
                '''.format(str(values_dict['teacher_id'])))

  secret = secret_query.fetchall()[0][1]

  # Step 2: check if secret is effectively equal:
  
  if secret != values_dict['secret']:
    return "error"
  exam = dbase.execute(''' 
                  UPDATE Exams
                  SET grade = {grade}
                  WHERE session_id = {session_id}
                  AND student_id = {student_id}        
                  '''.format(grade = values_dict['grade'], session_id = values_dict['session_id'], student_id = values_dict['student_id']))
  # Close the DB
  dbase.close()
  return True

############################################
# 4. Get all grades from a exam session
############################################

@app.get("/session_grades")
async def session_grades(payload: Request):
  values_dict = await payload.json()
  # Open the DB
  dbase = sqlite3.connect('tp10.db', isolation_level=None, check_same_thread=False)

  # Step 1: retrieve all the information about the session, underlying exams and students by joining the tables

  grades_query = dbase.execute(''' 
    SELECT Students.matricule, Exams.grade FROM Sessions
    LEFT JOIN Exams ON Exams.session_id = Sessions.id
    LEFT JOIN Students ON Exams.student_id = Students.id               
    WHERE Sessions.id  = {session_id}
    '''.format(session_id = str(values_dict['session_id'])))
  
  grades = grades_query.fetchall()

  # Step 2: clean the results

  # Close the DB
  dbase.close()
  return grades 

if __name__ == '__main__':
  uvicorn.run(app, host='127.0.0.1', port=8000)