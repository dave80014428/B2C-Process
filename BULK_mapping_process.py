#!/usr/bin/env python

"""BULK_mapping_process.py: This pulls in the data for the BULK BAN-to-customer mapping requests, scraping 
the issuetracker site for related tickets, downloading the latest files (for Requester Comments tickets)
moves those files to the BULK_mapping directory within the dated folder for today's BAN mapping requests
(if one exists), appends the ticket number to each spreadsheet, then validates the data spreadsheet
by spreadsheet, sends the ticket back for more info if the data in the file is invalid, and preps the data
for insertion into the ssms_issue_tracker_automation if it is valid and then stores the data in a csv """

__author__ = "Dave Curtis"
__copyright__ = "Copyright 2023, Dave Curtis"
__credits__ = ["Dave Curtis"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Dave Curtist"
__email__ = "dave.curtis1@lumen.com"
__status__ = "Development" # one of these: "Prototype", "Development", or "Production".

## -- IMPORTS ---- ##
#standard library
import os       
import requests
import random
import csv
import re ### regexp
import pyodbc
from time import sleep
import glob
from pathlib import Path
import shutil
import html # --> to decode the file names from html to unicode




#3rd party
import selenium
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import sys
import lxml
import glob
import subprocess #### to open the csv on mac
import itertools ### for ziplongest()
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
import cx_Oracle
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support import ui
import PySimpleGUI as sg
import dataGovComp as dgc
import pandas as pd
# ======================================================================================================


#GLOBALS ## ---------------------------------------------
bulk_mapping_requests_data_list  = []
bulk_mapping_ticket_numbers = []
invalid_bulk_template_list = []
requested_customers_list = []
fan_list = []
original_data_from_CHIT_list = []
complete_data_list = []
Global_fans_that_actually_exist = []
dupes_found_count = 0

bulk_status_requester_fname_dict = {}

containsMSG = []

ticket_state = 0 # if it is 0, then the ticket is set to feedback required. If it is 1 the it is not set to feedback required
list_type = 0 # if 0, then the list is BANs only - if it is 1, then the list is customers only

today = datetime.now().strftime('%m%d%Y') ## strftime changed datetime to string
LDrive_finance_data_gov_path = r'L:Finance_Data_Governance\Sales Issue Tracker Automation\BAN Mapping\\' + today + r' - CSVs for Dataloader'
LDrive_BULK_templates_path = r'L:Finance_Data_Governance\Sales Issue Tracker Automation\BAN Mapping\\' + today + r' - CSVs for Dataloader\BULK Templates'
oracle_lib_path = r'C:\ORACLE\instantclient_19_18'

thisLink = "<strong><a href='https://centurylink.sharepoint.com/sites/FinCustomerData/SitePages/Bulk%20Billing%20Account%20(BAN)%20Remapping%20Requests.aspx'>this link</a></strong>"
#FUNCTIONS ## -------------------------------------------
def getStringFromList(a_list):
    new_string = []
    new_string = str([str(x) for x in a_list]).strip("[]") # peel off the brckets so it is stored in a format SQL can parse
    
    return new_string



def tupleToList(a_tuple):
    new_list = [list(item) for item in a_tuple]

    return new_list



def queryOracle(data_string, num):
    conn = dgc.connectCDW()

    if num == 1:
        print("searching for fan info")
        print("here is the fan data string", data_string)
        query = (f"""SELECT 
                        BA.FINANCE_ACCOUNT_NBR 
                        ,CUST_NBR AS CURRENT_CUST_NBR
                        ,BA.BILL_ACCOUNT_NBR
                        ,CA.SALES_CHAN_NAME 
                    FROM 
                        CODS.BILLING_ACCOUNT BA
                        LEFT JOIN CODS.CUSTOMER_ATTRIBUTION CA USING(CUST_ODS_ID) -- LEFT JOIN to bring in all data even if NULL values
                    WHERE 1=1
                        AND FINANCE_ACCOUNT_NBR IN ({data_string})
                """)
    elif num == 2:
        print("finding the rest of rqst cust info")
        print("here is the cust data string", data_string)
        query = (f"""SELECT 
                        C.CUST_NBR AS RQSTD_CUST_NBR
                        ,C.CUST_NAME AS RQSTD_CUST_NAME
                        ,CA.SALES_CHAN_NAME AS RQSTD_CUST_SALES_CHAN
                    FROM 
                        CODS.CUSTOMER C
                        JOIN CODS.CUSTOMER_ATTRIBUTION CA USING(CUST_ODS_ID)
                    WHERE 1=1
                        AND c.CUST_NBR IN ({data_string})
            """)

    c = conn.cursor()
    results = c.execute(query).fetchall()
    c.close()

    print('here are the results', results)
    return tupleToList(results)


def queryMDM(data_string, num):
    conn = dgc.connectMDM()
    if num == 1:
        query = """
"""




def appendDataToCompleteList(fans, customers, ticket, data_list):
    global original_data_from_CHIT_list
    global complete_data_list
    global ticket_state
    global list_type
    global Global_fans_that_actually_exist
    
    fans_that_actually_exist = []

    ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist = {}
    tickets_with_fans_that_dont_exist = []

    fans_that_dont_exist_list = []

    customers_that_actually_exist = []
    customers_that_dont_exist_list = []
    tickets_with_customers_that_dont_exist = []
    ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist = {}

    row_order = [0, 1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 11, 15, 16, 10, 14, 9]

    #print("made it to the appendData portion")


    if len(fans) > 999:
        print("\n\nfans list is long")
        fan_data = []
        num_1k_blocs = len(fans) / 999
        print("this is the total of fans over 1k: " ,num_1k_blocs)
        #print("and here is the length: " , len(ssms_results))

        #test_num = 513
        num_1k_blocs = len(fans) // 999
        remainder = len(fans) % 999
        max_number_divisible = len(fans) - remainder
        #print(remainder)
        #print(num_1k_blocs)
        #print("this is max number: ", max_number_divisible)
        start_chunk = 0
        num = 0
        rev_query_results = []


        if num_1k_blocs:
            while num < num_1k_blocs:
            ##for chunk in range(num_1k_blocs):
                #print("this is the start_chunk: ", start_chunk)
                end_chunk = start_chunk + 999
                ranged_save_fans = []
                for number in range(start_chunk, end_chunk):
                    ranged_save_fans.append(fans[number])
                    #print(start_chunk, end_chunk)
                    #print(ranged_save_FANs)
                    #print("this is the number: ", number)
                fans_string = getStringFromList(ranged_save_fans)
                #fans_sql_string = fans_sql_string + fans_string
                ##print(query_3)
                fan_data.append(queryOracle(fans_string, 1))
                start_chunk = start_chunk + 999
                #print("this is the new start_chunk: ", start_chunk)
                if start_chunk == max_number_divisible:
                    ranged_save_fans = []
                    end_chunk = start_chunk + remainder
                    for number in range(start_chunk, end_chunk):
                        ranged_save_fans.append(fans[number])
                    fans_string = getStringFromList(ranged_save_fans)
                    #fans_sql_string = fans_sql_string + fans_string # append fans to the fans_sql_string so we can use that global variable later
                    fan_data.append(queryOracle(fans_string, 1))
                num += 1
                #print("this is the num: ", num)
                #print("chunk: ", start_chunk)
            fan_data = [fan_row for sublist in fan_data for fan_row in sublist]

        print("\nZAMBAZOO - here is the fan_data", fan_data)
        for fans_that_exist in fan_data:
            fan = fans_that_exist[0]
            if fan not in fans_that_actually_exist:
                fans_that_actually_exist.append(fan)
        '''
        for fan_row in fan_data:
            for fans_that_exist in fan_row:
                fan = fans_that_exist[0]
                if fan not in fans_that_actually_exist:
                    fans_that_actually_exist.append(fan)
        '''
        print("here are the fans that exist:", fans_that_actually_exist)  

    else:
        print("made it to the less than 1k check for fans")
        fans_string = getStringFromList(fans)


        fan_data = queryOracle(fans_string, 1)
        print("\nhere is the fan_data:", fan_data)
        for fans_that_exist in fan_data:
            fan = fans_that_exist[0]
            if fan not in fans_that_actually_exist:
                fans_that_actually_exist.append(fan)
        print("here are the fans that exist:", fans_that_actually_exist)  
        #fix tuples
    
        fan_data = [fan for fan in fan_data ]
        print("\nhere is the fan_data for under 1k entries:", fan_data)

    if len(customers) > 999:
        print("\n\ncust list is long")
        cust_data = []
        num_1k_blocs = len(customers) / 999
        print("this is the total customers over 1k: " ,num_1k_blocs)
        #print("and here is the length: " , len(ssms_results))

        #test_num = 513
        num_1k_blocs = len(customers) // 999
        remainder = len(customers) % 999
        max_number_divisible = len(customers) - remainder
        #print(remainder)
        #print(num_1k_blocs)
        #print("this is max number: ", max_number_divisible)
        start_chunk = 0
        num = 0
        rev_query_results = []


        if num_1k_blocs:
            while num < num_1k_blocs:
            ##for chunk in range(num_1k_blocs):
                #print("this is the start_chunk: ", start_chunk)
                end_chunk = start_chunk + 999
                ranged_save_customers = []
                for number in range(start_chunk, end_chunk):
                    ranged_save_customers.append(customers[number])
                    #print(start_chunk, end_chunk)
                    #print(ranged_save_FANs)
                    #print("this is the number: ", number)
                customer_string = getStringFromList(ranged_save_customers)
                #fans_sql_string = fans_sql_string + fans_string
                ##print(query_3)
                cust_data.append(queryOracle(customer_string, 2))
                start_chunk = start_chunk + 999
                #print("this is the new start_chunk: ", start_chunk)
                if start_chunk == max_number_divisible:
                    ranged_save_customers = []
                    end_chunk = start_chunk + remainder
                    for number in range(start_chunk, end_chunk):
                        ranged_save_customers.append(customers[number])
                    customer_string = getStringFromList(ranged_save_customers)
                    #fans_sql_string = fans_sql_string + fans_string # append fans to the fans_sql_string so we can use that global variable later
                    cust_data.append(queryOracle(customer_string, 2))
                num += 1
                #print("this is the num: ", num)
                #print("chunk: ", start_chunk)
            cust_data = [cust for sublist in cust_data for cust in sublist]

        print("\nFANDANGO - here is the cust_data", cust_data)
        for cust_that_exists in cust_data:
            cust = cust_that_exists[0]
            if cust not in customers_that_actually_exist:
                customers_that_actually_exist.append(cust)
        print("here are the customers that exist:", customers_that_actually_exist)
    else:
        print("made it to the less than 1k check for customers")
        rqstd_cust_string = getStringFromList(customers)

        cust_data = queryOracle(rqstd_cust_string, 2)

        for cust_that_exists in cust_data:
            cust = cust_that_exists[0]
            if cust not in customers_that_actually_exist:
                customers_that_actually_exist.append(cust)
        print("here are the customers that exist:", customers_that_actually_exist)
        cust_data = [cust for cust in cust_data]
        
        print("\nhere is the cust_data for under 1k entries:", cust_data)



    complete_row_index = 0
    for row in data_list:
        #print("\nhere is the data list row:", row)
        fan = row[10]
        rqstd_cust = row[11]
        ticket = row[0]
        #print(fan)
        #print("here is the requested cust", rqstd_cust)
        for fan_row in fan_data:
            #print("\nhere's the fan_row from the fan data just returned", fan_row)
            fan_from_fan_data = fan_row[0]
            #print("\n and here is the fan from the fan_row in fan_data: ", fan)
            if fan == fan_from_fan_data:
                #print("\nfound the fan!", fan, "here's the fanfrom fan data", fan_from_fan_data)
                fan_book = []
                fan_book.append(fan_row[1])
                fan_book.append(fan_row[2])
                fan_book.append(fan_row[3])
                row.extend(fan_book)
                #print("\n\n\nhere's the new data row", row)
        # for the fans that AREN'T in the FAN data (the ones that don't actually exist)
        if fan not in fans_that_actually_exist:
            print("this fan don't exist", fan)
            fans_that_dont_exist_list.append(fan)
            if ticket not in tickets_with_fans_that_dont_exist:
                tickets_with_fans_that_dont_exist.append(ticket)
                ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist[ticket] = [fan]

            elif ticket in tickets_with_fans_that_dont_exist:
                ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist[ticket].append(fan)
        #print("all bad tickets and bans", ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist)
        for cust_row in cust_data:
            #print("here's the cust_data:", cust_row[0])
            if cust_row[0] == rqstd_cust:
                cust_book = []
                cust_book.append(cust_row[1])
                cust_book.append(cust_row[2])
                row.extend(cust_book)
                #print("\n\nShould have the full cust info now", row)
        if rqstd_cust not in customers_that_actually_exist:
            #print("this cust don't exist", rqstd_cust)
            customers_that_dont_exist_list.append(rqstd_cust)
            if ticket not in tickets_with_customers_that_dont_exist:
                tickets_with_customers_that_dont_exist.append(ticket)
                ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist[ticket] = [rqstd_cust]
            elif ticket in tickets_with_customers_that_dont_exist:
                ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist[ticket].append(rqstd_cust)

    
    # print("and the complete data is as follows", complete_data_list)

    if ticket in ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist and ticket in ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist:
        bad_cust_and_bans_dict = {}
        bad_cust_and_bans_dict['bad_bans'] = ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist[ticket]
        bad_cust_and_bans_dict['bad_custs'] = ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist[ticket]
        ticket_state = 0 ## - > change to 1 in order to send a comment through the ticket, but complete review of the existing billing accounts
        return bad_cust_and_bans_dict
    elif ticket in ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist and ticket not in ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist:
        bad_bans_list = ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist[ticket]
        ticket_state = 0 ## - > change to 1 in order to send a comment through the ticket, but complete review of the existing billing accounts
        list_type = 0
        return bad_bans_list
    elif ticket in ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist and ticket not in ticket_and_complete_bad_bans_dict_for_bans_that_dont_exist:
        bad_rqstd_cust_list = ticket_and_complete_bad_requested_cust_dict_for_rqstd_custs_that_dont_exist[ticket]
        ticket_state = 0
        list_type = 1
        return bad_rqstd_cust_list
    else:
        for row in original_data_from_CHIT_list:
            #print("heres the row yo", row)
            fan = row[10]
            if fan in fans_that_actually_exist:
                # remove the row with the associated fan from the complete data list - if the FAN doesn't exist, there's no point in running it
                # --> through the rest of the process - let's knock it out here and add a comment about it. 
                #original_data_from_CHIT_list.pop(complete_row_index)
                #complete_row_index += 1
                
                #print("here is the row with the fan that exists row", row)
                #print("here is the row length", len(row))
                
                #row_order = [0, 1, 2, 3, 4, 5, 6, 7, 8, 11, 12, 10, 14, 15, 9, 13]
                new_row = [row[i] for i in row_order] # build the new row according to the indexes we need

                print('\n\nhere is the new row:', new_row)
                complete_data_list.append(new_row)
        return None




def querySSMS(): 
    conn = dgc.connectSSMS()

    global bulk_mapping_requests_data_list
    global bulk_status_requester_fname_dict
    # will query the issue tracker for all BULK request
    
    # save data as list of lists to bulk_mapping_requests_data_list
    query = (f"""
SELECT 
	M2.issue_id
	,M2.create_employee_nbr
		,U.firstname
		,U.lastname
	,IC.issue_id
		,comment
		,comment_ts
	,M2.status_id
		,'BULK' as TICKET_TYPE --'S.[name]' as STATUS_NAME -- 'New' or 'Requester Comments Added'
    ,M2.issue_root_cause_id
from
	[salesissuetracker].[dbo].[issue_comments] IC
	LEFT JOIN salesissuetracker.dbo.issue_m2 M2
		ON ic.issue_id = M2.issue_id
	LEFT JOIN salesissuetracker.dbo.issue_classification ICL
		ON M2.issue_classification_id = ICL.issue_classification_id
	LEFT JOIN salesissuetracker.dbo.[user] U
		ON M2.create_employee_nbr = U.employee_number
	LEFT JOIN salesissuetracker.dbo.[status] S
		ON M2.status_id = S.status_id
	LEFT JOIN salesissuetracker.dbo.busOrgs BO
		ON M2.source_cust_nbr = BO.cust_nbr
inner join 
	(select issue_id, min(comment_ts) as first_comment
	from [salesissuetracker].[dbo].[issue_comments]
	group by issue_id) cc
	on ic.issue_id = cc.issue_id
where 1=1 -- ic.issue_id = '20017766'   
	and issue_category_id = '9'
	and S.status_id in ('20', '26')
	and ic.comment_ts = cc.first_comment
	and icl.issue_classification_id = '143'""")

    c = conn.cursor()
    results = c.execute(query).fetchall()
    c.close()

    bulk_mapping_requests_data_list = tupleToList(results)

    #append ticket numbers to bulk_mapping_ticket_numbers
    for x in bulk_mapping_requests_data_list: bulk_mapping_ticket_numbers.append(x[0])
    for x in bulk_mapping_requests_data_list: bulk_status_requester_fname_dict[x[0]] = x[2] #ticket key, first name value

    return bulk_mapping_requests_data_list



def chromeDriverRunANDSetOptions():
    chromeOptions = webdriver.ChromeOptions()
    #options.headless = True
    chromeOptions.add_argument("start-maximized")
    chromeOptions.add_experimental_option("excludeSwitches", ["enable-automation"])
    chromeOptions.add_experimental_option('useAutomationExtension', False)
    #chromeOptions.add_experimental_option("prefs", { "download.default_directory": LDrive_BULK_templates_path, "download.directory_upgrade": True})

    driver = webdriver.Chrome(options=chromeOptions) #(ChromeDriverManager().install(), chrome_options = options) # perhaps unnecessary now?

    return driver
    



def evaluateRow(csv_data, boilerplate, saved_issue_tracker_query_results_row, file_name):
    global original_data_from_CHIT_list

    this_ticket_data_list_only = []
    row_count = 2

    validation_comment = ''

    #fan_pattern adjusted on 12/3/2024 to include 5-XXXXX format
    fan_pattern = r'^(.+(\-[A-Z]+|\-[A-Z]+(2E|2E_FED))|[51]\-([A-Z0-9]+))$' 
    cust_pattern = r'^([1-4]{1}\-[A-Z0-9]*(-.*)?|\d+)$'
    data_cleanse_pattern = r'(\"|\s)+' #("|\n|\s|\t|\r)+
    blankExcelRegex = r'^(\s?|\'{1})$'
    
    for row in csv_data:
        #print(row)
        #remove all white space characters (external and internal and including new lines, tabs, etc.)
        fan_clean_fromCSV = re.sub(data_cleanse_pattern, '', row[0])
        cust_clean_fromCSV = re.sub(data_cleanse_pattern, '', row[1])
        # print("cust and fan", fan_clean_fromCSV, cust_clean_fromCSV)
        if re.match(fan_pattern, fan_clean_fromCSV) and re.match(cust_pattern, cust_clean_fromCSV):

            # use this opportunity to build a FANs list and rqstd customers list and make sure to
            ## only add those fans/ customers that don't already exist in either list (don't want to have dupe data)
            if fan_clean_fromCSV not in fan_list:
                fan_list.append(fan_clean_fromCSV)
            else:
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                f"A blank field was provided at this row in the CUST_NBR column. Please correct the field in the Template "\
                "and attach the corrected Template to this ticket.\n\nDon't forget to search the file for (and correct) all other errors " \
                "before doing so."\
                f"\n{boilerplate}"
            if cust_clean_fromCSV not in requested_customers_list:
                requested_customers_list.append(cust_clean_fromCSV)
            new_cleaned_csv_data_row = [fan_clean_fromCSV, cust_clean_fromCSV]
            saved_row = []
            saved_row = saved_issue_tracker_query_results_row + new_cleaned_csv_data_row
            original_data_from_CHIT_list += [saved_row]
            this_ticket_data_list_only += [saved_row]

            


        elif re.match(fan_pattern, fan_clean_fromCSV) and not re.match(cust_pattern, cust_clean_fromCSV):
            # the regex ^(\s?|\'{1})$ will hopefully help me to avoid finding errors where random cells that appear
            ## clear in Excel actually aren't. In other words, they may have blank space characters, or a single apostrophe (which shows
            # as a blank in Excel)
            if cust_clean_fromCSV is None or cust_clean_fromCSV == '' or cust_clean_fromCSV == ' ' or re.match(blankExcelRegex, cust_clean_fromCSV):
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                f"A blank field was provided at this row in the CUST_NBR column. Please correct the field in the Template "\
                "and attach the corrected Template to this ticket.\n\nDon't forget to search the file for (and correct) all other errors " \
                "before doing so."\
                f"\n{boilerplate}"
            else:
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                f"{cust_clean_fromCSV} was provided in the CUST_NBR column, but it is not a Customer Number. "\
                "Please correct the field in the Template and attach the corrected Template to this ticket.\n\nDon't forget to search the file " \
                "for (and correct) all other errors before doing so."\
                f"\n{boilerplate}"
        elif not re.match(fan_pattern, fan_clean_fromCSV) and re.match(cust_pattern, cust_clean_fromCSV):
            if fan_clean_fromCSV is None or fan_clean_fromCSV == '' or fan_clean_fromCSV == ' ' or re.match(blankExcelRegex, fan_clean_fromCSV):
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                f"A blank field was provided at this row in the FINANCE_ACCOUNT_NBR column. Please correct the field in the Template "\
                "and attach the corrected Template to this ticket.\n\nDon't forget to search the file for (and correct) all other errors " \
                "before doing so."\
                f"\n{boilerplate}"
            else:
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}:\n "\
                f"{fan_clean_fromCSV} was provided in the FINANCE_ACCOUNT_NBR column, but it is not a Finance Account Number. "\
                f"Please correct the field in the Template and attach the corrected Template to this ticket.\n\nDon't forget to search "\
                "the file for (and correct) all other errors before doing so."\
                f"\n{boilerplate}"
        else:
            if (fan_clean_fromCSV is None or fan_clean_fromCSV == '' or fan_clean_fromCSV == ' ' or re.match(blankExcelRegex, fan_clean_fromCSV)) \
                and (cust_clean_fromCSV is None or cust_clean_fromCSV == '' or cust_clean_fromCSV == ' ' or re.match(blankExcelRegex, cust_clean_fromCSV) ):
                continue
                ## no need for us to do anything here since we don't care if the entire row is basically empty - its probably a deletion error
            else:
                print("didn't find the fan", fan_clean_fromCSV)
                print(re.match(fan_pattern, fan_clean_fromCSV))
                validation_comment = f"While parsing '{file_name}', the automation came across an error in row {row_count}:\n "\
                f"{fan_clean_fromCSV} was provided in the FINANCE_ACCOUNT_NBR column (column A), but it is not a Finance Account Number. " \
                f"And {cust_clean_fromCSV} was provided in the CUST_NBR column (column B), but it is not a Customer Number. "\
                "Please correct both fields in the Template and attach the corrected Template to this ticket. Don't forget to search "\
                "the file for (and correct) all other errors before doing so."\
                f"\n{boilerplate}"
        row_count += 1
    return validation_comment



# this validates that the data in each attachment is accurate.
# it will evaluate all rows of data, as soon as it hits an invalid entry the entire file is labeled invalid
def validateAttachment(file_path, ticket_num, file_name):
    global LDrive_BULK_templates_path
    global invalid_bulk_template_list
    global bulk_mapping_requests_data_list
    global original_data_from_CHIT_list
    global list_type
    global dupes_found_count
    # call a new fan_list so each ticket can be resolved separately (instead of a global ticket to aggregate all FANs, this will allow us
    ## --> to find out if any of the fans in this ticket exist or not - if not, then generate a new comment, but also add the completed
    ## --> list (all the ban/cust info) to the original_data_from_CHIT_list global so that, at least will have everything we need to review
    ## --> the tickets through the main process. Then, mark the ticket as Under Review 
    fan_list = [] 
    requested_customers_list = []
    this_ticket_data_list_only = []

    bot_names = ["Bishop", "Bender", "WALL-E", "Optimus", "HAL", "R2-D2", "Bumblebee", "Linguo", "Gort", "Johnny 5", "Marvin", \
                "KITT", "H.E.L.P.eR", "GLaDOS", "Data"]
    
    random.seed()
    strongAtThisLink = "<strong><a href='https://centurylink.sharepoint.com/sites/FinCustomerData/SitePages/Bulk%20Billing%20Account%20(BAN)%20Remapping%20Requests.aspx'>at this link</a></strong>"
    strongTheDocumentationLink = "<strong><a href='https://centurylink.sharepoint.com/sites/FinCustomerData/SitePages/Bulk%20Billing%20Account%20(BAN)%20Remapping%20Requests.aspx'>the documentation</a></strong>"
    strongLink = "<strong><a href='https://centurylink.sharepoint.com/sites/FinCustomerData/SitePages/Bulk%20Billing%20Account%20(BAN)%20Remapping%20Requests.aspx'>How to Create a Bulk Billing Account to Customer Mapping Request</a></strong>"
    thisLink = "<strong><a href='https://centurylink.sharepoint.com/sites/FinCustomerData/SitePages/Bulk%20Billing%20Account%20(BAN)%20Remapping%20Requests.aspx'>this link</a></strong>"
    emailLink = "<strong><a href='mailto:customerhierarchy@centurylink.com?subject=Bulk Customer Status Change Request'>CustomerHierarchy@centurylink.com</a></strong>"

    introBoiler =   f"<p style='color:red;font-weight:bold;'>\nBEEP BOOP BOOP BEEP!</p>\nHello, {bulk_status_requester_fname_dict[ticket_num]}. I am a Data Governance Robot. My name is {random.choice(bot_names)}.\n\n"

    boilerplate =   "\nPlease note that any attachments you provide will be reviewed by "\
                    f"the automation for the correct naming convention, formatting, and data composition as outlined {strongAtThisLink} ."\
                    "\n\nThe first mistake the automation finds will automatically send the ticket "\
                    "back to you to correct, so if there may be further issues within your attachment it is up to you "\
                    "to find and correct them or risk correcting/completing the Template each time an issue is found. "\
                    f"\n\nIf, after reading through the documentation (available here: {strongLink} ), you still have questions, concerns, or suggestions, please " \
                    f"reach out to the Data Governance team at {emailLink}"

        ##,'' as CURRENT_CUST_NBR
#		,'' AS BILL_ACCOUNT_NBR
#		,'' AS RQSTD_CUST_NBR
#		,'' AS RQSTD_CUST_NAME
#		,'' AS RQSTD_CUST_SALES_CHANNEL
#		,'' AS FINANCE_ACCOUNT_NBR
#	,'' AS CURRENT_CUST_SALES_CHANNEL

    validation_comment = ''
    fan_pattern = r'^(.+(\-[A-Z]+|\-[A-Z]+(2E|2E_FED))|[51]\-([A-Z0-9]+))$' # any character except for line terminators one or more times, followed by a group (at the end) that includes only letters
    cust_pattern = r'^([1-4]{1}\-[A-Z0-9]*(-.*)?|\d+)$'
    data_cleanse_pattern = r'(\"|\s)+' #("|\n|\s|\t|\r)+
    blankExcelRegex = r'^(\s?|\'{1})$'

    blank_cell_flag = 'n'


    if file_path.endswith('.csv') and file_name[0:21].upper() == "BULK_MAPPING_TEMPLATE": # if the file is a CSV and is named correctly
        print("\n\neverything's looking good!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        #df = pd.read_csv(file_path, encoding = "utf-8", index_col = False)
        for data_row in bulk_mapping_requests_data_list:
            #print("this is row[0]", row[0], data_row[0])
            if data_row[0] == ticket_num:
                saved_issue_tracker_query_results_row = data_row
                break
        saved_issue_tracker_query_results_row
        print("here is the save issue tracker query results row: " , saved_issue_tracker_query_results_row)

        #open and read the CSV data into list    
        with open(file_path, newline='', encoding='utf-8-sig') as opened_file:    #utf-8-sig finding this ï»¿ at the beginning of Headers. without the encoding. weird eh? 
            reader = csv.reader(opened_file)
            csv_data = list(reader)

            i=1
            total_rows_in_file = [i + 1 for row in csv_data]
            print("here are the total rows:", total_rows_in_file)
        '''
        print("here is the csv_data before pandas:", csv_data)

        df = pd.read_csv(file_path, header=0, encoding='utf-8-sig')

        print("here is the df: ", df)

        # Drop all rows that contain any blank values
        df = df.dropna(axis='columns', how='all')

        print("here is the df after dropping blank columns: ", df)

        # Drop all columns that contain any blank values
        df = df.dropna(axis='rows', how='all')

        print("here is the df after dropping blank rows: ", df)

        csv_data = [df.columns.values.tolist()] + df.values.tolist()

        print("here is the csv data: ", csv_data)
        '''


        #row 1 is the Header row - so row 2 is the first row of regular data 
        ## --> not in a regular indexed sort of way, but a looking-at-the-spreadsheet sort of way. You wouldn't call the header row "row 0"
        ## --> when you're describing the sheet to someone. It's not even labeled that way. It would be labeled 1 on the left-hand side
        ## --> ya dig? 
        row_count = 2

        #header check
        if len(csv_data[0]) == 2:
            fan_header = re.sub(data_cleanse_pattern, '', csv_data[0][0]) # stripping off all white space - don't want to do too much editing to compare
                                                        ## --> we expect the requesters to at least get the names right - 
                                                        ## --> extra white space we can deal with - not a deal breaker, but the 
                                                        ## --> Template has everything edited perfectly already, so we wouldn't
                                                        ## --> expect the requester to drift too far from that Template
                                                        ## -- some whitespace, maybe, is understandable... but actually misnaming? nah
            custNum_header = re.sub(data_cleanse_pattern, '', csv_data[0][1])
            print("headers:", fan_header, custNum_header)

            if fan_header == 'FINANCE_ACCOUNT_NBR' and custNum_header == 'CUST_NBR':
                print("made it past the header check")
                csv_data.pop(0) # remove headers from review
                # reset count on num dupes found between tickets reviewed
                dupes_found_count = 0
                for row in csv_data:
                    if len(row) > 2: 
                        test_list = []
                        for item in row:
                            if re.match(blankExcelRegex, item) or item == '': #if no data in the cell then skip it
                                continue
                            elif (item != '' and item is not None): #if item exists (is not null) 
                                test_list.append(item)

                        if len(test_list) > 2:
                            print("here are the items in this row: ", len(row), item)

                            validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered extra columns. Please note that "\
                            f"the automation only accepts two columns in your Bulk Mapping Template. Just the FINANCE_ACCOUNT_NBR column "\
                            f"(column A) and the CUST_NBR column (column B). \n{boilerplate}"

                            break #break the loop so the validation comment goes through
                        else:
                            #validation_comment = evaluateRow(csv_data, boilerplate, saved_issue_tracker_query_results_row, file_name)
                            blank_cell_flag = 'y' # this flag will let us decide whether to move the file data on to the next if statement below or not. 

                    if len(row) == 2 or blank_cell_flag == 'y':
                        #print(row)
                        #remove all white space characters (external and internal and including new lines, tabs, etc.)
                        fan_clean_fromCSV = re.sub(data_cleanse_pattern, '', row[0])
                        cust_clean_fromCSV = re.sub(data_cleanse_pattern, '', row[1])
                        # print("cust and fan", fan_clean_fromCSV, cust_clean_fromCSV)
                        if re.match(fan_pattern, fan_clean_fromCSV) and re.match(cust_pattern, cust_clean_fromCSV):

                            # use this opportunity to build a FANs list and rqstd customers list and make sure to
                            ## only add those fans/ customers that don't already exist in either list (don't want to have dupe data)
                            if fan_clean_fromCSV not in fan_list:
                                fan_list.append(fan_clean_fromCSV)
                            else:
                                dupes_found_count += 1
                            if cust_clean_fromCSV not in requested_customers_list:
                                requested_customers_list.append(cust_clean_fromCSV)
                            new_cleaned_csv_data_row = [fan_clean_fromCSV, cust_clean_fromCSV]
                            saved_row = []
                            saved_row = saved_issue_tracker_query_results_row + new_cleaned_csv_data_row
                            original_data_from_CHIT_list += [saved_row]
                            this_ticket_data_list_only += [saved_row]
                            #print("\n\n here is the original data from chit list: ", this_ticket_data_list_only)

                            


                        elif re.match(fan_pattern, fan_clean_fromCSV) and not re.match(cust_pattern, cust_clean_fromCSV):
                            # the regex ^(\s?|\'{1})$ will hopefully help me to avoid finding errors where random cells that appear
                            ## clear in Excel actually aren't. In other words, they may have blank space characters, or a single apostrophe (which shows
                            # as a blank in Excel)
                            if cust_clean_fromCSV is None or cust_clean_fromCSV == '' or cust_clean_fromCSV == ' ' or re.match(blankExcelRegex, cust_clean_fromCSV):
                                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                                f"A blank field was provided at this row in the CUST_NBR column. Please correct the field in the Template "\
                                "and attach the corrected Template to this ticket.\n\nDon't forget to search the file for (and correct) all other errors " \
                                "before doing so."\
                                f"\n{boilerplate}"
                            else:
                                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                                f"{cust_clean_fromCSV} was provided in the CUST_NBR column, but it is not a Customer Number. "\
                                "Please correct the field in the Template and attach the corrected Template to this ticket.\n\nDon't forget to search the file " \
                                "for (and correct) all other errors before doing so."\
                                f"\n{boilerplate}"
                        elif not re.match(fan_pattern, fan_clean_fromCSV) and re.match(cust_pattern, cust_clean_fromCSV):
                            if fan_clean_fromCSV is None or fan_clean_fromCSV == '' or fan_clean_fromCSV == ' ' or re.match(blankExcelRegex, fan_clean_fromCSV):
                                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across an error in row {row_count}. "\
                                f"A blank field was provided at this row in the FINANCE_ACCOUNT_NBR column. Please correct the field in the Template "\
                                "and attach the corrected Template to this ticket.\n\nDon't forget to search the file for (and correct) all other errors " \
                                "before doing so."\
                                f"\n{boilerplate}"
                            else:
                                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across an error in row {row_count}:\n "\
                                f"{fan_clean_fromCSV} was provided in the FINANCE_ACCOUNT_NBR column, but it is not a Finance Account Number. "\
                                f"Please correct the field in the Template and attach the corrected Template to this ticket.\n\nDon't forget to search "\
                                "the file for (and correct) all other errors before doing so."\
                                f"\n{boilerplate}"
                        else:
                            if (fan_clean_fromCSV is None or fan_clean_fromCSV == '' or fan_clean_fromCSV == ' ' or re.match(blankExcelRegex, fan_clean_fromCSV)) \
                                and (cust_clean_fromCSV is None or cust_clean_fromCSV == '' or cust_clean_fromCSV == ' ' or re.match(blankExcelRegex, cust_clean_fromCSV) ):
                                continue
                                ## no need for us to do anything here since we don't care if the entire row is basically empty - its probably a deletion error
                            else:
                                print("didn't find the fan", fan_clean_fromCSV) 
                                print(re.match(fan_pattern, fan_clean_fromCSV))
                                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across an error in row {row_count}:\n "\
                                f"{fan_clean_fromCSV} was provided in the FINANCE_ACCOUNT_NBR column (column A), but it is not a Finance Account Number. " \
                                f"And {cust_clean_fromCSV} was provided in the CUST_NBR column (column B), but it is not a Customer Number. "\
                                "Please correct both fields in the Template and attach the corrected Template to this ticket. Don't forget to search "\
                                "the file for (and correct) all other errors before doing so."\
                                f"\n{boilerplate}"
                        row_count += 1
                    else:
                        print("here is the particular row: ", row)
                        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered fewer columns than needed " \
                            "in order to analyze your request. Please note that "\
                            f"the automation must find two columns in your Bulk Mapping Template: the FINANCE_ACCOUNT_NBR column "\
                            f"(column A) and the CUST_NBR column (column B). \n{boilerplate}"

            elif fan_header != 'FINANCE_ACCOUNT_NBR' and custNum_header == 'CUST_NBR':
                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across a naming error with your headers. "\
                    f"Column A is named incorrectly. It should be named 'FINANCE_ACCOUNT_NBR' but is named '{fan_header}' instead. " \
                    f"Please review the information {strongAtThisLink}\n\nBe sure to look for correct naming convention and data formatting requirements for "\
                    "the BULK_MAPPING_TEMPLATE and then take care to resolve any issues within your file before you attach it again." \
                    f"\n{boilerplate}"
            elif fan_header == 'FINANCE_ACCOUNT_NBR' and custNum_header != 'CUST_NBR':
                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across a naming error with your headers. "\
                    f"Column B is named incorrectly. It should be named 'CUST_NBR' but is named '{custNum_header}' instead. " \
                    f"Please review the information {strongAtThisLink}\n\nBe sure to look for the correct naming convention and data formatting requirements for "\
                    "the BULK_MAPPING_TEMPLATE and take care to resolve any issues within your file before you attach it again. " \
                    f"\n{boilerplate}"
            else:
                validation_comment = f"{introBoiler}While parsing '{file_name}', the automation came across a naming error with your headers. "\
                    f"Both column A and Column B are incorrectly named. Column A should be named 'FINANCE_ACCOUNT_NBR' "\
                    f"but is named '{fan_header}' instead.\nColumn B should be named 'CUST_NBR' but is named '{custNum_header}' instead." \
                    f"Please review the information {strongAtThisLink}\n\nBe sure to look for the correct naming convention and data formatting requirements for "\
                    "the BULK_MAPPING_TEMPLATE and take care to resolve any issues within your file before you attach it again." \
                    f"\n{boilerplate}"
        else:
            print("here is the troubling row of trouble: ", csv_data[0])
            validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered fewer columns than needed " \
                            "in order to analyze your request. Please note that "\
                            f"the automation must find two columns in your Bulk Mapping Template: the FINANCE_ACCOUNT_NBR column "\
                            f"(column A) and the CUST_NBR column (column B). \n{boilerplate}"

    elif file_path.endswith('.csv') and file_name[0:21] != "BULK_MAPPING_TEMPLATE":
        validation_comment = f"{introBoiler}The file you've attached is not named as specified by the Data Governance team; therefore, "\
            "it cannot be parsed by our automation and your request has not been analyzed.\n\nPlease be sure to download the " \
            f"necessary Template {strongAtThisLink}\n\nThen review the Template requirements outlined there. " \
            f"If the data you've provided is accurate according to our rules (available {strongAtThisLink}) and "\
            "only the file name is incorrect, then please correct the file name, save the file as a CSV and attach it (again) to "\
            f"this request. \n{boilerplate}"
    elif not file_path.endswith('.csv') and file_name[0:21] == "BULK_MAPPING_TEMPLATE":
        validation_comment = f"{introBoiler}The file you've attached is not a CSV, which does not meet the specifications outlined "\
            "by the Data Governance team; therefore, it has not been analyzed. "\
            f"\n\nPlease review the information {strongAtThisLink}\n\nIf the data you've provided in your spreadsheet is " \
            f"accurate according to the rules (available at the same link: {strongLink}) and you only need the correct file type, then save the file as a CSV and " \
            f"attach it (again) to this ticket. \n{boilerplate}"
    else: 
        validation_comment = f"{introBoiler}The file you've attached is:\n1 - not named correctly\n--and--\n2 - not a CSV.\nTherefore, it has not been analyzed. "\
            f"\n\nPlease review the information {strongAtThisLink}\n\nThen, download the necessary Template and fill it out according to the " \
            "directions provided. "\
            f"Once you've completed the Template (as directed {strongAtThisLink}), be sure to save it as a CSV and attach it to this ticket.\n{boilerplate}"
    
    if fan_list and requested_customers_list and dupes_found_count == 0:
        # if the data has made it through the  checks above, 
        ## --> then we can find out if these billing accounts exist
        bad_bans_list_or_dict = appendDataToCompleteList(fan_list, requested_customers_list, ticket_num, this_ticket_data_list_only)
        # if the number of bad bans (non existing bans) equals the total rows in the document, that means none of the billing accounts
        # --> provided actually exist in CODS.BA, which means the request can't be governed
        # --> and it needs to head back to the creator to fix
        if bad_bans_list_or_dict:

            if isinstance(bad_bans_list_or_dict, dict):

                #create lists from the dictionary data sent over - change 'em to string values, then format 'em to add to ticket comment
                bad_bans_list = bad_bans_list_or_dict['bad_bans']
                bad_cust_list = bad_bans_list_or_dict['bad_custs']
                bad_bans_list_string = (str(x) for x in bad_bans_list)
                bad_cust_list_string = (str(x) for x in bad_cust_list)
                #print("\n\nhere is the bad bans list string", bad_bans_list_string)
                bad_bans_list_string_formatted = "\n".join(bad_bans_list_string) if bad_bans_list_string else '' #get 'dem sweet sweet new lines
                bad_cust_list_string_formatted = "\n".join(bad_cust_list_string) if bad_cust_list_string else ''
                if len(bad_bans_list) == len(total_rows_in_file) and len(bad_cust_list) > 0:
                    validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that none of the billing accounts " \
                        "provided in the template exist in CODS.BILLING_ACCOUNT / CODS.CUSTOMER which means we could not govern your request. " \
                        f"Here's the list of customers that could not be found:\n{bad_cust_list_string_formatted}"\
                        f"Please read through the requirements {strongAtThisLink} for more information. \n{boilerplate}"
                elif len(bad_bans_list) > 0:
                    validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that some of the billing accounts " \
                        f"in the provided template do not exist in CODS.BILLING_ACCOUNT and some of the customers do not exist in CODS.CUSTOMER " \
                        "which means the desired mapping for each of those rows (whether populated by non-existent customers or " \
                        "non-existent billing accounts) could not be analyzed or governed. Here is a list of the billing accounts that could not "\
                        f"be found:\n{bad_bans_list_string_formatted}\n\nThe rest of the billing accounts in the template appear to exist, "\
                        "but the file will not be sent through the analysis engine until each billing account in the template " \
                        f"also exists in CODS.BILLING_ACCOUNT.\nAnd here is a list of the customers that could not be found: " \
                        f"\n{bad_cust_list_string_formatted}\n\nSimilar to the billing accounts above, each time the automation discovers " \
                        "a customer in the spreadsheet that does not exist in CODS.CUSTOMER, the spreadsheet will be sent back to you to correct." \
                        f"\n\nWith that in mind, please read through the requirements {strongAtThisLink} for more information."

            else:
                if list_type == 0:
                    bad_bans_list = bad_bans_list_or_dict
                    bad_bans_list_string = (str(x) for x in bad_bans_list)
                    bad_bans_list_string_formatted = "\n".join(bad_bans_list_string) if bad_bans_list_string else '' #get 'dem sweet sweet new lines
                    if len(bad_bans_list) == len(total_rows_in_file):
                        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that none of the billing accounts " \
                            "provided in the template exist in CODS.BILLING_ACCOUNT which means we could not govern your request. " \
                            f"Please read through the requirements {strongAtThisLink} for more information \n{boilerplate}"
                    elif len(bad_bans_list) > 0:
                        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that some of the billing accounts " \
                            f"in the provided template do not exist in CODS.BILLING_ACCOUNT which means their mapping could not be analyzed: "\
                            f"\n{bad_bans_list_string_formatted}\n\nThe rest of the billing accounts in the template appear to exist, "\
                            "but the file will not be sent through the analysis engine until each billing account in the template " \
                            "also exists in CODS.BILLING_ACCOUNT.\n\nPlease read through "\
                            f"the requirements {strongAtThisLink} for more information \n{boilerplate}"
                    elif len(bad_bans_list) == 0:
                        validation_comment = ''
                else:
                    bad_cust_list = bad_bans_list_or_dict
                    bad_cust_list_string = (str(x) for x in bad_cust_list)
                    bad_cust_list_string_formatted = "\n".join(bad_cust_list_string) if bad_cust_list_string else '' #get 'dem sweet sweet new lines
                    if len(bad_cust_list) == len(total_rows_in_file):
                        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that none of the customers " \
                            "provided in the template exist in CODS.CUSTOMER which means we could not govern your request. " \
                            f"Please read through the requirements {strongAtThisLink} for more information \n{boilerplate}"
                    elif len(bad_cust_list) > 0:
                        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered that some of the customers " \
                            f"in the provided template do not exist in CODS.CUSTOMER which means the requested mapping could not be analyzed: "\
                            f"\n{bad_cust_list_string_formatted}\n\nThe rest of the customers in the template appear to exist, "\
                            "but the file will not be sent through the analysis engine until each customer in the template " \
                            "also exists in CODS.CUSTOMER. So there is the potential for this ticket to be sent back to you to correct "\
                            "multiple times, if you are not careful to confirm the each customer (and billing account) that you provide." \
                            f"\n\nPlease read through the requirements {strongAtThisLink} for more information \n{boilerplate}"
                    elif len(bad_bans_list) == 0:
                        validation_comment = ''
        else:
            print("No list produced... da fuq?")
    ### --> if the file contains duplicate FANs (and after checking for every other problem in existence) shoot the file back to the user 
    #### ---> to remove 'em
    elif fan_list and requested_customers_list and dupes_found_count > 0:
        validation_comment = f"{introBoiler}While parsing '{file_name}', the automation discovered duplicate entries in the FINANCE_ACCOUNT_NBR column (column A). "\
            "Please delete the entire row for each duplicate within the template, save the file, then attach it again to this ticket. "\
            "\n\nDon't forget to search the file for (and correct) all other errors before doing so."\
            f"\n{boilerplate}"

    return validation_comment
    


def workWithElement(driver, element, ticket):
    global ticket_state
    evaluation = ''
    
    # get downloaded file name
    print("working with the element")
    bulk_template_name = html.unescape(element.get_attribute('innerHTML').replace('&nbsp;', ' ')).strip()
    bulk_template_name = bulk_template_name.replace("\u00ad", '_')
    print("the bulk template name taken from the website: ", bulk_template_name)
    bulk_template_file_type = os.path.splitext(bulk_template_name)[1]

    #element[0].location_once_scrolled_into_view
    #driver.execute_script("arguments[0].scrollIntoView();", element[0])
    driver.execute_script("arguments[0].click();",element)
    #element[0].click()
    sleep(random.randrange(10,15)) # give it time to download
    list_of_files = glob.glob(r'C:\Users\AC79386\Downloads\*') # * means all if need specific format then *.csv
    latest_file_path = max(list_of_files, key=os.path.getctime)
    latest_file_name_from_dwnlds = os.path.basename(latest_file_path) #Path(latest_file_path).stem # returns file name without the extension

    just_file_name_from_file_path_noExtension = re.sub('\s', ' ', latest_file_name_from_dwnlds[0:len(bulk_template_name[:-len(bulk_template_file_type)])])
    just_file_name_from_ticket_noExtension = re.sub('\s', ' ' , bulk_template_name[:-len(bulk_template_file_type)])
    ## having issues with white spaces in these names (above) - this should sort 'em. i'm hopeful

    
    '''for x in range(len(just_file_name_from_file_path_noExtension)):
        if just_file_name_from_file_path_noExtension[x] == just_file_name_from_ticket_noExtension[x]:
            print("here's dem chars:",just_file_name_from_file_path_noExtension[x],  just_file_name_from_ticket_noExtension[x] )
        else:
            print("Welp, thse chars don't match:",just_file_name_from_file_path_noExtension[x],  just_file_name_from_ticket_noExtension[x] )'''
        

    if just_file_name_from_file_path_noExtension == just_file_name_from_ticket_noExtension:
        print("names match!", latest_file_name_from_dwnlds, 'then', bulk_template_name )
        evaluation = validateAttachment(latest_file_path, ticket, latest_file_name_from_dwnlds)
    else:
        print("names don't match I guess", latest_file_name_from_dwnlds[0:len(bulk_template_name)], 'then', bulk_template_name )


    if evaluation:
        #status_change = driver.find_element(By.XPATH, "//button[normalize-space()='Change Status']")
        ## --> AS OF 4/13/2023 the decision is to deny any ticket that comes through with even a single billing account that doesn't exist in CODS.BA
        if ticket_state == 0:
            print("apparently this is an invalid submission", evaluation)
            this_bool = True
            while this_bool:
                try:
                    comments = driver.find_element(By.ID, 'comments')
                    comments.send_keys(evaluation)
                    this_bool = False             
                except:
                    print("couldn't find the comment box")
                    driver.refresh
                    sleep(random.randrange(3,5))
        elif ticket_state == 1: ## --> if ticket state is 1 then it means that some (not all) of the attached billing accounts don't exist in CODS.BA
            print("apparently this ticket is not invalid - just missin some bans", evaluation)
            this_bool = True
            while this_bool:
                try:
                    comments = driver.find_element(By.ID, 'comments')
                    comments.send_keys(evaluation)
                    this_bool = False             
                except:
                    print("couldn't find the comments for an invalid ticket")
                    driver.refresh
                    sleep(random.randrange(3,5))
            ## --> add ticket number to the front of their file name
            to_file_name = str(ticket) + '_' + latest_file_name_from_dwnlds
            
            # new file path
            new_file_path = LDrive_BULK_templates_path + fr'\{to_file_name}'

            # check if file exits in the L drive - if it do, delete it
            if os.path.isfile(new_file_path):
                os.remove(new_file_path)
                shutil.move(f'{latest_file_path}', f'{new_file_path}' ) 
            else:
                # save files to L: Drive
                shutil.move(f'{latest_file_path}', f'{new_file_path}' )  

    else:
        ## --> add ticket number to the front of their file name
        to_file_name = str(ticket) + '_' + latest_file_name_from_dwnlds
        
        # new file path
        new_file_path = LDrive_BULK_templates_path + fr'\{to_file_name}'

        # check if file exits in the L drive - if it do, delete it
        if os.path.isfile(new_file_path):
            os.remove(new_file_path)
            shutil.move(f'{latest_file_path}', f'{new_file_path}' ) 
        else:
            # save files to L: Drive
            shutil.move(f'{latest_file_path}', f'{new_file_path}' )  
 



def find_MSG_attachments(elements, ticket):
    global containsMSG

    for element in elements:
        if re.match('^.*\.msg$', element.get_attribute('innerHTML')):
            containsMSG.append(ticket)
            break




def scrapeCHITforLatestTemplates():
    global bulk_mapping_ticket_numbers
    global thisLink

    #make this file path if it does not already exist
    if not os.path.exists(LDrive_finance_data_gov_path):
        os.mkdir(LDrive_finance_data_gov_path)
    if not os.path.exists(LDrive_BULK_templates_path):
        os.mkdir(LDrive_BULK_templates_path)


    driver = chromeDriverRunANDSetOptions()
    
    #js_code = "arguments[0].scrollIntoView();"
    # scrape chit tool based on the bulk mapping ticket #s above
    bulk_mapping_ticket_numbers = [20037973,20038042,20038048,20038074,20038087]#TEST TEST only
    for ticket in bulk_mapping_ticket_numbers:
        driver.get(f'https://salesissuetracker.corp.global.level3.com/#/ticket/{ticket}') # removed to test 3/25/2023
        
        sleep(random.randrange(15,20))
        #element = driver.find_elements(By.XPATH, "//a[@class='btAttachmentDownload ng-binding']")[1]
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.XPATH, "//button[normalize-space()='Add Attachment']")))
        get_files_button = driver.find_element(By.CSS_SELECTOR, '.fa.fa-paperclip.text-primary.push-5-l.push-5-r')
        sleep(random.randrange(1,3))

        control_bool = True
        while control_bool:
            try:
                get_files_button.click()
                control_bool = False             
            except:
                driver.refresh
                sleep(random.randrange(3,5))
        sleep(random.randrange(3,5))
        try:
            element = driver.find_element(By.XPATH, "//p[@class='font-s13']//a[@class='btAttachmentDownload ng-binding']")
        ## --> old - finding the first downloadable attachment driver.find_elements(By.XPATH, "//div[@class='font-s13']//a[@class='btAttachmentDownload ng-binding']")
        # check that there is at least one matching element and click it
        except:
            no_attachment_eval =    "There is no attachment. All bulk billing account mapping requests require a completed "\
                                    "Bulk Mapping Template in order to govern the request. "\
                                    f"\n\nPlease visit {thisLink}, download the Bulk Mapping Template and fill it out according to the rules "\
                                    "outlined therein." 
            element = ''
        if element: 
            find_all_attachments = driver.find_elements(By.XPATH,"//p[@class='font-s13']//a[@class='btAttachmentDownload ng-binding']")
            find_MSG_attachments(find_all_attachments, ticket)

            workWithElement(driver, element, ticket)

        else:
            attempts = 2
            i = 0
            while i < attempts:
                try:
                    element = driver.find_elements(By.XPATH, "/html[1]/body[1]/div[1]/div[1]/main[1]/div[1]/div[1]/div[4]/div[1]/div[3]/ul[1]/li[1]/div[1]/p[2]/a[1]")
                    workWithElement(element)           
                except:
                    driver.refresh
                    sleep(random.randrange(3,5))
                    i += 1
            control_bool = True
            while control_bool:
                try:
                    comments = driver.find_element(By.ID, 'comments')
                    comments.send_keys(no_attachment_eval)
                    control_bool = False             
                except:
                    driver.refresh
                    sleep(random.randrange(3,5))
            
        sleep(random.randrange(10,20))





def main():
    #global variables imported to local scope
    global LDrive_finance_data_gov_path
    global oracle_lib_path
    global requested_customers_list
    global fan_list
    global original_data_from_CHIT_list
    global complete_data_list

    #dgc.findOracleLib(oracle_lib_path)
    #dgc.lDriveTest(LDrive_finance_data_gov_path)

    querySSMS()
    scrapeCHITforLatestTemplates()

    print(complete_data_list)

    return complete_data_list
    
    

#this code will execute if this file is run explicitly. If this is imported as a module, we'll only have access
## to the functions and classes defined herein - with dot-notation access, like bmp.main() for example.
### so, th plan would be to run all functions in main, import this module and then call it in the 
#### ssms_issue_tracker automation file, where the return would be same info that we get inthe
##### ssms_issue_tracker_automation querySSMS() function, then the data can run through all the same
###### processes
if __name__ == "__main__":
    main()
