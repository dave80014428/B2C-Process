#!/usr/bin/env python

"""dataGovComp.py: A module for common Data Governance functions: oracle and MSsqlServer connections L:Drive search, etc."""

__author__ = "Dave Curtis"
__copyright__ = "Copyright 2023, Dave Curtis"
__credits__ = ["Dave Curtis"]
__license__ = "GPL"
__version__ = "1.0"
__maintainer__ = "Dave Curtist"
__email__ = "dave.curtis1@lumen.com"
__status__ = "Production"

#standard library
import os     
import sys  
import re ### regexp

#3rd party
import sys
import cx_Oracle
import PySimpleGUI as sg
import pyodbc


def lDriveTest(path):
    x = 0
    while x == 0:
        thisPath = os.path.exists(path) 
        try:
            if not thisPath:
                os.makedirs(path)
                #print("made a path")
                break
            elif thisPath:
                #print("We're up n' running, baby!")
                break
        except Exception as e:
            # lDrivePop()
            #print("nuh uh")
            lDriveError = ('Please connect to the L Drive.\nThen click "Check L Drive."\nClose this window, or click "Close" to end the program.')
            errorPopup= [
                            [sg.Text(lDriveError, size=(40, 1))],
                            [sg.Button('Check L Drive', key='-CHECK_L_DRIVE-'), sg.Button('Close')]
                        ]
            errorCPNIpopup = sg.Window('L Drive Error', errorPopup, modal=True)
            event, values = errorCPNIpopup.read(close=True)
            if event == sg.WIN_CLOSED or event == 'Close': # if user closes window or clicks cancel
                #print("closing the window")
                break
            elif re.match('^-CHECK_L_DRIVE-$', event):
                #print("made it to match check")
                x == 0
                #print(x) 


def findOracleLib(path):
    print("here is the oracle path:", path)
    try:
        cx_Oracle.init_oracle_client(lib_dir=f"{path}")
    except Exception as err:
        print("Couldn't find Oracle Library!")
        print(err)
        sys.exit(1)


def connectSSMS():
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=USIDCVSQL0251;"
                                   "Database=salesissuetracker;"
                                   "UID=CUSTHIER;"
                                   "PWD=SchebangCUSTH!er0072024$")
    return conn



# connect to sql server table using windows authentication. The secret sauce is Trusted_Connection=True
def connectSSMS_0213_AR_TABLE():
    conn = pyodbc.connect("Driver={SQL Server};"
                                   "Server=USIDCVSQL0213;"
                                   "Database=CFS_Reporting;"
                                   "Trusted_Connection=True") # use Trusted_Connection=True for windows authentication
    
    #test block below 10/9/2024
    #query = f"""
    #            SELECT acctNum FROM [dbo].[Modified Segments_ExtendedBuckets_CFS_PMT] WHERE acctNum in ('5-SFXKHHLK-A', '6517351988324-CRISE', '5-BLQ3GSYY-A', '334176394-ENS') 
    #        """
    
    #c = conn.cursor()
    #results = c.execute(query).fetchall()
    #c.close()
    #print(results)

    #results = [fan for row in results for fan in row]
    return conn


def connectSSMS_QUARTERLY_B2C_REQUESTS_TABLE():
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                                   "Server=usodcvsql0260.ctl.intranet,7114;"
                                   "Database=DATA_GOVERNANCE;"
                                   "UID=CUSTHIER;"
                                   "PWD=f3!rZi8*oN49wb7")
    
    return conn
    


def connectIDG01P():
    dsn_tns = cx_Oracle.makedsn(r'RACORAP32-SCAN.CORP.INTRANET', '1521', service_name='SVC_IDG01P')
    conn = cx_Oracle.connect(user='CUSTHIER', password='CarmineH20#2024', dsn=dsn_tns)

    # query = """
    #         Select
    #             CONCAT(ACCOUNT_NUMBER, '-ENS') as ENS_FINANCE_ACCOUNT_NBR,
    #             CONCAT(LEGACY_IDENTIFIER, CONCAT('-', SOURCE_SYSTEM)) as CRIS_FINANCE_ACCOUNT_NBR
    #         from MDMRULE.LKP_BAN_XREF_ENS
    #         Where ACCOUNT_NUMBER in('333146816','334167449')
    #     """

    # c = conn.cursor()
    # results = c.execute(query).fetchall()
    # c.close()
    # print(results)

    # results = [list(row) for row in results]
    # return results

    return conn



def connectCDW():
    dsn_tns = cx_Oracle.makedsn(r'RACORAP16-SCAN.IDC1.LEVEL3.COM', '1521', service_name='CDW01P_USERS')
    conn = cx_Oracle.connect(user='AC79386', password='VerilyHaut22341#?', dsn=dsn_tns)



    #query = """ SELECT
     #                   FINANCE_ACCOUNT_NBR
     #                   ,TO_CHAR(GL_PERIOD_START_DT,'YYYYMM') AS GL_PERIOD_YM
     #                   ,ROUND(SUM(USD_CURM_AMT),2) AS SUM_USD
     #               FROM 
     ##                   DSL_FINANCE.F_REVENUE_DETAIL_ALL FRDA
     #                   INNER JOIN CODS_FINANCE.GL_ACCOUNT GA ON FRDA.GL_ACCOUNT_ODS_ID = GA.GL_ACCOUNT_ODS_ID
     #                   AND GA.GL_ACCOUNT_TYP = 'Revenues'
     #               WHERE 1=1
     #                   AND GL_PERIOD_START_DT >= add_months(trunc(sysdate,'mm'),-13)
     #                   AND FRDA.journal_source_cd in ('BR','BA')                           
     #                   AND FINANCE_ACCOUNT_NBR IN ('5-WGRGSMGP-A')
     #               GROUP BY FRDA.FINANCE_ACCOUNT_NBR, FRDA.GL_PERIOD_START_DT
     #               -- order by  FRDA.FINANCE_ACCOUNT_NBR, FRDA.GL_PERIOD_START_DT desc"""
    
    #c = conn.cursor()
    #results = c.execute(query).fetchall()
    #c.close()
    #print(results)

    #results = [fan for row in results for fan in row]
    #return results

    return conn



def connectMDM():
    dsn_tns = cx_Oracle.makedsn(r'RACORAP32-SCAN.CORP.INTRANET', '1521', service_name='SVC_IDG01P')
    conn = cx_Oracle.connect(user='AC79386', password='CarmineH20#2024', dsn=dsn_tns)

    return conn


def get1kBlocksFromList(list: list, divider: int) -> dict:
    num_1k_blocks = len(list) // divider
    remainder = len(list) % divider
    max_number_divisible = len(list) - remainder
    
    # print("heres the complete list: ", list)    
    # print("len of list: ", len(list))
    # print("remainder after dividing by divider: ", remainder)
    # print("total number of 1k blocks to work with: ", num_1k_blocks)
    # print("this is max number: ", max_number_divisible)

    start_chunk = 0
    num = 0
    ranged_save_data =  {}
    
    # if there actually is an amount of fans over 1k
    if num_1k_blocks:
        while num < num_1k_blocks:
            end_chunk = start_chunk + divider 
            ranged_save_data[num] = list[start_chunk:end_chunk]

            num += 1
            start_chunk = end_chunk

            if start_chunk == max_number_divisible:
                ranged_save_data[num] = list[start_chunk:]
    else:
        ranged_save_data[0] = list

    # print("all the data for chunked data: ", ranged_save_data)
    return ranged_save_data



def main():
    print("Hello DG")
    # print(connectSSMS_0213_AR_TABLE())
    # print(connectCDW())
    # print(connectSSMS_QUARTERLY_B2C_REQUESTS_TABLE()) 
    print(connectIDG01P())  



if __name__ == "__main__":
    main()