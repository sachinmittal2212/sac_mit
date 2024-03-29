import json
from pandas.core.frame import DataFrame
import cx_Oracle
import pandas as pd
import sys
from datetime import datetime

def lambda_function(str accountCode):
    # TODO implement
    db=cx_Oracle.connect("CORP-SVC-IFT", "invr$invr",dns_tns)
    cursor = db.cursor()
    cursor.execute("select procedure_name from all_procedures where object_name = 'PK_IVZ_IFT_EXTRACT' ")
    rows = cursor.fetchall()
    procedureName = ['PK_IVZ_IFT_EXTRACT.'+str(list(rows[indexRow])[0]) for indexRow in range(0,len(list(rows))-1)]
    
    # To convert AccountAuM procedure to JSON format
    l_cur_AuM = cursor.var(cx_Oracle.CURSOR)
    ret_cursor_AuM  = cursor.callproc(procedureName[0],(l_cur_AuM,accountCode))
    dfx_AuM = pd.DataFrame(ret_cursor_AuM[0])
    dfx_AuM.columns = ['accountCode', 'accountCurrency', 'investment','cash','aum']
    result_AuM = dfx_AuM.to_dict()
    return result_AuM

def lambda_handler(event, context):
    logger.info('Event: %s', event)
    response = lambda_function(event['TOUHI'])
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }