# -*- coding: utf-8 -*-
"""
Created on Tue Aug  2 16:05:40 2022

@author: Sebastian Dudek
"""

import requests
import pandas as pd
import datetime
import numpy as np
import sys
from simple_salesforce import Salesforce
import time



class RaiseNow:

    def __init__(self,data_start,data_end,limit=2000):


        self._data_start = data_start
        self._data_end = data_end
        self._limit = limit

    @property
    def data_start(self):
        return self._data_start

    @property
    def data_end(self):
        return self._data_start

    @property
    def limit(self):
        return self.limit


    def download_transaction_list(self):
        """
        Funkcja pobiera dane z RaiseNow. Ustaw liczę rekordów.
        Dane są posortowane od najnowszych darowizn.
        """
#
        transaction_list = requests.get(
            """https://api.raisenow.com/epayment/api/"""+
            """************/transactions/search?sort[0][order]=ascendig""",
            params={'records_per_page': self._limit},
            auth=('************', '***************')).json()

        transaction_list=pd.DataFrame(
            transaction_list['result']['transactions'])


        transaction_list = transaction_list[transaction_list.last_status==
                                            'final_success']

        transaction_list.created=transaction_list.created.apply(lambda x:
                datetime.datetime.fromtimestamp((int(x)  )).strftime('%Y-%m-%d')   )

        transaction_list=transaction_list[(transaction_list.created >=
                self._data_start) & (transaction_list.created < self._data_end) ]

        return transaction_list



    def download_transaction(self):

        transaction_list=self.download_transaction_list()

        transaction=[]

        for i in transaction_list['epp_transaction_id']:

            r=requests.get('https://api.raisenow.com/epayment/api/transaction/status',
                params={'transaction-id':i, 'merchant-config': '*********'},
                auth=('*****************', '**************'))
            transaction.append(r.json())

        transaction=pd.DataFrame(transaction)

        transaction.created=transaction.created.apply(lambda x:
                  datetime.datetime.strptime(x[:10], '%Y-%m-%d')  )

        transaction=transaction[(transaction.created >
                self._data_start) & (transaction.created < self._data_end) ]

        if transaction.empty:
            sys.exit()
        else:
             transaction = transaction[ transaction.test_mode == 'false' ]
             return transaction


    def convert_columns(self):

        df=self.download_transaction()

        if df.empty:
            sys.exit()
        if 'stored_rnw_recurring_interval_text' not in df.columns:

            df['stored_rnw_recurring_interval_text']='brak'






        df=df[[
        'stored_customer_firstname', 'stored_customer_lastname', 'stored_customer_street',
        'stored_customer_street2','stored_customer_street_number','stored_customer_city',
        'stored_customer_zip_code','stored_customer_email','stored_customer_email_permission',
        'stored_customer_birthdate','created','amount','stored_rnw_recurring_interval_text']]



        df.columns= [ 'FirstName','LastName','MailingStreet','MailingStreet2',
        'MailingStreet3','MailingCity','MailingPostalCode',	'Email','Do_Not_Mail__c',
        'Birthdate','Transaction_Date__c', 'Transaction_Amount__c', 'is_recurring__c' ]

        return df



    def get_data(self):

        df=self.convert_columns()


        # Walidacja danych do salesforce

        df.is_recurring__c = df.is_recurring__c.apply(lambda x:
                                            False if ( str(x)=='nan' or str(x)=='brak')  else True)
        df.Transaction_Amount__c=df.Transaction_Amount__c.apply(lambda x:
                                                        float(x)/100.0)

        df.Birthdate=df.Birthdate.apply(lambda x: np.nan if str(x).find('/') >0 else x)



        df.Birthdate=df.Birthdate.apply(lambda x: np.nan if len(str(x))!=10  else x)

        df.Transaction_Date__c=df.Transaction_Date__c.apply(
            lambda x: str(x)[:11])

        df.MailingStreet = df[['MailingStreet','MailingStreet2','MailingStreet3']].apply(
            lambda x: ' '.join(x.dropna().values.tolist()) ,    axis=1  )

        df=df.drop(['MailingStreet2',	'MailingStreet3' ], axis=1)
        df.MailingStreet=df.MailingStreet.apply(
            lambda x: str(x).replace(',', ' '))



        df.insert(11, 'S__c', 'Accepted')
        df.insert(12, 'Source__c', 'Płatność on-line')
        df.insert(13, 'Type__c', 'DOTACJA')
        df.insert(14, 'Description__c', 'Raisenow')
        df.insert(15, 'Filename__c', 'Raisenow'+'_'+ str(datetime.date.today()))

        return df






class Import_Contact_RaiseNow:





    def df_contakt(self):


        df_contakt = self.all_data()
        df_contakt = df_contakt[ ['FirstName', 'LastName', 'MailingStreet',
                'MailingCity', 'MailingPostalCode', 'Email', 'Do_Not_Mail__c',
                'Birthdate','is_recurring__c' ]]

        df_contakt.Do_Not_Mail__c=df_contakt.Do_Not_Mail__c.apply(
            lambda x: 1 if x=='true' else 0 )
        df_contakt.Birthdate=df_contakt.Birthdate.apply(
            lambda x: np.nan if str(x).find('/') >0   else x)
        df_contakt.Birthdate=df_contakt.Birthdate.apply(
            lambda x: np.nan if len(str(x))!=10  else x)
        df_contakt.Birthdate = np.where((pd.isna(df_contakt['Birthdate']
                                    )==True),None ,df_contakt['Birthdate'] )
        df_contakt.Email = np.where((pd.isna(df_contakt['Email']
                                    )==True),None ,df_contakt['Email'] )
        return df_contakt

    def salesforce_existing_emails(self):
        sf  =  Salesforce ( username = '****************' ,
        password = '***************' ,  security_token = '********************8' )

        sf_email=pd.DataFrame(sf.query_all(
        "SELECT Id , Email from contact where Email like '_%'  ")['records'])


        return sf_email

    def existing_contacts(self):
        existing_contacts = pd.merge(self.salesforce_existing_emails(),
                                     self.df_contakt(), on='Email')
        existing_contacts = existing_contacts.iloc[:,1:]
        return existing_contacts

    def new_contacts(self):

        new_con = self.df_contakt().set_index('Email').join(
            self.existing_contacts().set_index('Email'),
            lsuffix='_caller', rsuffix='_other')


        new_con= new_con[ new_con.Id.isnull() ].iloc[ :, :8]
        new_con=new_con.reset_index()

        new_con.columns =  [ 'Email','FirstName', 'LastName', 'MailingStreet',
                        'MailingCity','MailingPostalCode',  'Do_Not_Mail__c',
                        'Birthdate','LeadSource' ]


        new_con.LeadSource= new_con.LeadSource.apply(
            lambda x: 'RC WEB' if str(x)=='True' else 'RaiseNow OneOff')

        today_str=str(datetime.datetime.today())[:10]


        new_con.insert(9, 'AccountId', '*****')
        new_con.insert(10, 'OwnerId', '*****')

        new_con.insert(11, 'Lead_creation_Date__c',str(today_str) )

        new_con=new_con.drop_duplicates(subset=['Email'])

        new_con.Lead_creation_Date__c.apply(lambda x: str(x))
        new_con.LeadSource.apply(lambda x: str(x)   )


        return new_con

    def import_new_contacts(self):
        sf  =  Salesforce ( username = '*************8' ,
        password = '***************' ,
        security_token = '************' )

        log_import=[]
        if not self.new_contacts().empty:
            new_import=self.new_contacts().to_dict('records')
            new_import = [    new_import[x:x+7] for x in range(0, len(new_import), 7)  ]

            for i in new_import:

                f=log_import=sf.bulk.Contact.insert(i)

                log_import.append(f)
                time.sleep(10)
        else:
            time.sleep(10)

        return log_import

    def import_existing_contacts(self):

        sf  =  Salesforce ( username = '********************' ,
                           password = '**********************8' ,
                           security_token = '***********************' )

        existing_contacts = self.existing_contacts().iloc[:,:-1]
        existing_contacts=existing_contacts.drop_duplicates(subset=['Email'])


        log_import=[]

        if not self.existing_contacts().empty:
            existing_contacts=existing_contacts.to_dict('records')
            existing_contacts = [    existing_contacts[x:x+25] for x in
                                 range(0, len(existing_contacts), 25)  ]

            for i in existing_contacts:

                import_sf_e=log_import=sf.bulk.Contact.upsert(i, 'Id')

                log_import.append(import_sf_e)
                time.sleep(10)
        else:
            time.sleep(10)

        return log_import

    def total_imports(self):

        con_n=self.import_existing_contacts()
        time.sleep(20)
        con_e=self.import_new_contacts()
        total=[con_n,con_e]
        return total


    @staticmethod

    def all_data():
        sf  =  Salesforce ( username = '********************' ,
                   password = '****************' ,
                   security_token = '**************************' )
        transakcje=pd.DataFrame(sf.query_all("SELECT Id, Transaction_Date__c FROM "
        "Transaction__c where  Rachunek__c ='RAISENOW' and Transaction_Date__c > 2022-02-07 ")['records'])

        start_date = transakcje['Transaction_Date__c'].max()
        data_end = str(datetime.datetime.today())[:10]
        all_data = RaiseNow(start_date,data_end).get_data()

        return all_data




class Import_transaction_RaiseNow:





    def df_transaction(self):


        df_transaction = self.all_data()
        df_transaction=df_transaction[['Email','Transaction_Date__c','Transaction_Amount__c',
                        'is_recurring__c','S__c', 'Source__c', 'Type__c',
                        'Description__c', 'Filename__c']]
        return df_transaction


    @staticmethod
    def all_data():
        sf  =  Salesforce ( username = '**********' ,
                   password = '*************' ,
                   security_token = '**************' )
        transakcje=pd.DataFrame(sf.query_all("SELECT Id, Transaction_Date__c FROM "
        "Transaction__c where  Rachunek__c ='RAISENOW' and Transaction_Date__c > 2022-02-07 ")['records'])

        start_date = transakcje['Transaction_Date__c'].max()
        data_end = str(datetime.datetime.today())[:10]
        all_data = RaiseNow(start_date,data_end).get_data()


        return all_data


    def salesforce_existing_emails(self):
        sf  =  Salesforce ( username = '************' ,
        password = '*************' ,  security_token = '***************' )

        sf_email=pd.DataFrame(sf.query_all(
        "SELECT Id , Email from contact where Email like '_%'  ")['records'])

        return sf_email

    def map_id_sf(self):
        map_id_sf = pd.merge(self.salesforce_existing_emails(),
                                     self.df_transaction(), on='Email')
        map_id_sf= map_id_sf[['Id',  'Transaction_Date__c',
       'Transaction_Amount__c', 'is_recurring__c', 'S__c', 'Source__c',
       'Type__c', 'Description__c', 'Filename__c']]

        map_id_sf.columns= ['Contact_Payment__c',  'Transaction_Date__c',
       'Transaction_Amount__c', 'is_recurring__c', 'S__c', 'Source__c',
       'Type__c', 'Description__c', 'Filename__c']
        map_id_sf.Transaction_Date__c = map_id_sf.Transaction_Date__c.apply(lambda x: str(x)[:10] )



        return map_id_sf


    def import_transaction(self):
        transaction = self.map_id_sf()

        sf  =  Salesforce ( username = '************' ,
        password = '*************' ,  security_token = '***************' )



        log_import=[]
        if not transaction.empty:
            transaction=transaction.to_dict('records')
            transaction = [    transaction[x:x+7] for x in range(0, len(transaction), 7)  ]

            for i in transaction:

                f=log_import=sf.bulk.Transaction__c.insert(i)

                log_import.append(f)
                time.sleep(15)
        else:
            time.sleep(15)

        return log_import

    def import_all_data(self):

        contact_n=Import_Contact_RaiseNow().import_new_contacts()

        time.sleep(20)

        contact_e = Import_Contact_RaiseNow().import_existing_contacts()

        time.sleep(20)

        transaction = self.import_transaction()



run=Import_transaction_RaiseNow().import_all_data()










