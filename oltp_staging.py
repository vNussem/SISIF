# import warnings
import db_connections as dbConn
import read_from_file as rff
import pandas as pd
from tqdm import tqdm
import numpy as np
import json
import numpy

import string
import re

import dateutil.parser

import importlib

# import datetime
from datetime import datetime, time
# import timestamp

# path = "python\data-sync\etl-objects\chestionar-mtpl\\"

# print("oltp-staging")

#etlPath = "python\data-sync\etl-objects"

import decimal


import sys
sys.path.append("C:/DevTools/Visual Studio/python/data-sync/" )

# warnings.simplefilter(action='ignore', category=FutureWarning)

def checkSums(path, sourceConnection, destinationConnection):

    etlObject = rff.getJson(path + "object.json")

    if ("integrityTrigger" in etlObject):
        integrityTrigger = importlib.import_module("etl-objects." + etlObject["desc"] + "." + etlObject["integrityTrigger"]["name"]) 
        integrityTrigger.execute(etlObject)

    if ("checkOltp" not in etlObject or "checkStaging" not in etlObject or "checkResult" not in etlObject):
        return

    columns = []
    for column in etlObject["checkResult"]:
        columns.append(column["name"])

    oltpCursor = sourceConnection.cursor()    
    oltp_data = pd.DataFrame.from_records(oltpCursor.execute(etlObject["checkOltp"]).fetchall(), columns=columns)

    stagingCursor = destinationConnection.cursor()
    staging_data = pd.DataFrame.from_records(stagingCursor.execute(etlObject["checkStaging"]).fetchall(), columns=columns)
    # column_Names  = cursor.description
        
    for column in etlObject["checkResult"]:
        column["oltp"] = oltp_data[column["name"]][0].astype(np.float64)
        column["staging"] = staging_data[column["name"]][0].astype(np.float64)
        column["dif"] = (oltp_data[column["name"]][0] - staging_data[column["name"]][0]).astype(np.float64)
        
        if column["dif"] < 0:            
            if ("manualDeleteNotExisting" not in etlObject or etlObject["manualDeleteNotExisting"] == False):
                etlObject["crtKey"] = etlObject["resetKey"]
                etlObject["resetDate"] = datetime.isoformat(datetime.now())    

    
    file1 = open(path + "object.json", "w")
    file1.writelines(json.dumps(etlObject, indent=4))
    file1.close()

    summaryObject = rff.getJson(path + "/../summary.json")
    summaryObject[etlObject["desc"]] = etlObject["checkResult"]
    
    file1 = open(path + "/../summary.json", "w")
    file1.writelines(json.dumps(summaryObject, indent=4))
    file1.close()    


def execute(path):

    etlObject = rff.getJson(path + "object.json")
    sysdate = datetime.now()

    if ("reset_hours" in etlObject and "resetKey" in etlObject and datetime.now().time().hour in etlObject["reset_hours"]):
        if ("resert_days" not in etlObject or "resert_days" in etlObject and datetime.now().day in etlObject["resert_days"]):            
            etlObject["crtKey"] = etlObject["resetKey"]
            etlObject["resetDate"] = datetime.isoformat(sysdate)          
            

            file1 = open(path + "object.json", "w")
            file1.writelines(json.dumps(etlObject, indent=4))
            file1.close()


    if ("working_hours" in etlObject and datetime.now().time().hour not in etlObject["working_hours"]):
        return

    if ("working_days" in etlObject and datetime.now().day not in etlObject["working_days"]):
        return

    sourceConnection = dbConn.getConnectionByName(
        etlObject["sourceConnection"])
    destinationConnection = dbConn.getConnectionByName(
        etlObject["destinationConnection"])

    fetchsize = etlObject["fetchsize"]
    pk = etlObject["pk"]

    # print(query)

    ofOfRowsCursor = sourceConnection.cursor()
    query = etlObject["noOfRows"].replace(":crtKey", etlObject["crtKey"])
    # noOfRows = ofOfRowsCursor.execute(
    #     etlObject["noOfRows"], crtKey=etlObject["crtKey"]).fetchall()[0][0]
    if ("parameters" in etlObject):
        for p in etlObject["parameters"]:
            query = query.replace("[" + p["name"] + "]", p["value"])


    noOfRows = ofOfRowsCursor.execute(query).fetchall()[0][0]

    query = rff.getText(
        path + etlObject["query"]).replace(":crtKey", etlObject["crtKey"])
    if ("parameters" in etlObject):
        for p in etlObject["parameters"]:
            query = query.replace("[" + p["name"] + "]", p["value"])

    sorceCursor = sourceConnection.cursor()
    # sorceCursor.prepare(query)

    # sorceCursor.execute(query, crtKey=etlObject["crtKey"])
    sorceCursor.execute(query)

    columnTypes = {"staging_id": None}

    sourceColumns = []
    columns = ["staging_id"]
    for column in sorceCursor.description:
        columns.append(column[0].lower())
        sourceColumns.append(column[0].lower())
        columnTypes[column[0].lower()] = column[1]

    # print(",".join(columns))

    query = ""
    query_check_key_exists = ""

    destinationCursor = destinationConnection.cursor()
    deleteCursor = destinationConnection.cursor()
    insertCursor = destinationConnection.cursor()

    if ("destinationTable" in etlObject):
        query = "select " + ",".join(columns) + " from " + \
            etlObject["destinationTable"] + \
                " where (" + ",".join(pk) + \
            ") in ({0}) and deleted_date is null"
    
        query_check_key_exists = "update " + \
            etlObject["destinationTable"] + \
            " set check_key_exists = :checkKeyExists " + \
            " where (" + ",".join(pk) + \
            ") in ({0}) and deleted_date is null"

    
        destinationCursor.prepare(query)

    
        deleteCursor.prepare(
            "update " + etlObject["destinationTable"] + " set deleted_date = sysdate where staging_id = :staging_id")

        insertColumns = "inserted_date"
        values = "sysdate"
        for column in columns:
            insertColumns += ("" if insertColumns == "" else ", ") + column
            if (column == "staging_id"):
                values += ("" if values == "" else ", ") + \
                    etlObject["stagingId"]
            # elif (columnTypes[column] == datetime.datetime):
            elif (columnTypes[column] == datetime):
                values += ("" if values == "" else ", ") + \
                    "to_timestamp(:" + column + ", 'yyyy-mm-dd hh24:mi:ss.ff')"
            else:
                values += ("" if values == "" else ", ") + ":" + column

        if("checkKeyExists" in etlObject and etlObject["checkKeyExists"]):
            insertColumns +=  ("" if insertColumns == "" else ", ") + "check_key_exists"
            values += ("" if values == "" else ", ") + ":checkKeyExists"


        
        insertCursor.prepare("insert into " + etlObject["destinationTable"] + " (" +
                            insertColumns + ") values (" + values + ")")

    with tqdm(total=noOfRows, desc=etlObject["desc"] if "desc" in etlObject else "No desc") as pbar:
        # while noOfRows > 0:
        while True:
            pks = []
            rows = sorceCursor.fetchmany(fetchsize)
            # oltp_data = pd.DataFrame(rows)
            if (len(rows) == 0):
                break

            oltp_data = pd.DataFrame.from_records(rows, columns=sourceColumns)

            # oltp_data = oltp_data.replace('', None)

            # oltp_data = pd.DataFrame(
            #     np.where(oltp_data == '', None, oltp_data))

            for column in columns:
                if (column != "staging_id" and columnTypes[column] == str):
                    oltp_data[column] = oltp_data[column]\
                        .str.encode(encoding="ascii", errors="replace")\
                        .str.decode('utf-8', errors="ignore")

                    oltp_data[column] = pd.DataFrame(
                        np.where(oltp_data[column].str.len() == 0, None, oltp_data[column]))

                    # oltp_data = pd.DataFrame(np.arange(8).reshape(2, 4), columns=list('ABCD'))

                    # if (oltp_data.shape[0] == 0):
                    #     oltp_data = pd.DataFrame(columns=sourceColumns)
                    # else:
                    #     oltp_data.columns = sourceColumns
                    # oltp_data = pd.DataFrame(columns=sourceColumns)
                    # for row in rows:
                    #     oltp_data.append([col for col in row])

            for index, row in oltp_data.iterrows():
                pkValue = ()
                for pkColumn in pk:
                    if (type(row[pkColumn.lower()]) == pd._libs.tslibs.timestamps.Timestamp):
                        pkValue += ("**to_date('" + str(row[pkColumn.lower()]) + "', 'yyyy-mm-dd hh24:mi:ss. ')**",)
                    else:
                        pkValue += (row[pkColumn.lower()],)
                pks.append(pkValue)
                noOfRows -= 1
                pbar.update(1)

            # dest = destinationCursor.execute(None, ",".join(pks))
            # "SELECT * FROM Genre WHERE id in ({0})".format(', '.join('?' for _ in ids))


            query_format = ",".join([str(p).replace(",)", ")").replace('"**', '').replace('**"', '') for p in pks])
            
            # Verificare key sterse/disparute din OLTP
            if("checkKeyExists" in etlObject and etlObject["checkKeyExists"]):
                # destinationCursor.execute(
                #     query_check_key_exists.format(",".join([str(p).replace(",)", ")") for p in pks])))
                destinationCursor.execute(query_check_key_exists.format(query_format), {"checkKeyExists": sysdate})

            #End verificare

            
            # print(query_format)
            dest_data = pd.DataFrame()
            if ("destinationTable" in etlObject):
                destinationCursor.execute(query.format(query_format))
                dest_data = pd.DataFrame(destinationCursor.fetchall())

            if(destinationCursor.rowcount > 0):
                dest_data.columns = columns
            else:
                dest_data = pd.DataFrame(columns=columns)
            # dest_data.columns = [x[0] for x in destinationCursor.description]
            dest_data.columns = dest_data.columns.str.lower()

            # to do
            for column in columns:
                if (columnTypes[column] == decimal.Decimal):
                    # dest_data[column] = dest_data.apply(
                    #     lambda r: r[column].apply(str).apply(
                    #         decimal.Decimal) if r[column] != None else None,
                    #     axis=1)
                    # dest_data[column] = dest_data[column].where(
                    #     (dest_data[column] != None), dest_data[column].apply(str).apply(decimal.Decimal))
                    dest_data.loc[~dest_data[column].isnull(), column] = dest_data.loc[~dest_data[column].isnull(
                    )][column].apply(str).apply(decimal.Decimal)
                # if (columnTypes[column] == datetime.date):
                #     dest_data.loc[~dest_data[column].isnull(), column] = dest_data.loc[~dest_data[column].isnull(
                #     )][column].apply(str).apply(datetime.date)

            # end to do

            checkColuns = []
            for ck in sourceColumns:
                if (not "ignoreChangesForColumns" in etlObject or ck not in etlObject["ignoreChangesForColumns"]):
                    checkColuns.append(ck)
            
            # diff_data = pd.concat([oltp_data, dest_data]).drop_duplicates(subset=sourceColumns, keep=False)
            diff_data = pd.concat([oltp_data, dest_data]).drop_duplicates(subset=checkColuns, keep=False)

           

            delete_data = []
            insert_data = []

            # deleted_keys = []

            for index, diff_row in diff_data.iterrows():
                stagingId = diff_row["staging_id"]
                if (not np.isnan(stagingId) and int(stagingId) > 0):
                    delete_data.append((int(stagingId), ))
                    
                    # if ("deleteTriggers" in etlObject):
                    #     pkKey = {}
                    #     for pkcol in etlObject["pk"]:                        
                    #         if (diff_row[pkcol] == datetime and diff_row[pkcol] != None):                            
                    #             pkKey[pkcol] = str(diff_row[pkcol])
                    #         else:
                    #             pkKey[pkcol] = diff_row[pkcol]                                                    
                    #     deleted_keys.append(pkKey)                        
                else:
                    insertRow = ()
                    for column in diff_row.iteritems():
                        columnValue = column[1]

                        if (type(columnValue) == np.int64):
                            columnValue = int(columnValue)
                        if (pd.isna(columnValue)):
                            columnValue = None
                        # if (type(columnValue) == pd.Timestamp):
                        # if (columnTypes[column[0]] == datetime.datetime and columnValue != None):
                        if (columnTypes[column[0]] == datetime and columnValue != None):
                            columnValue = str(columnValue)
                        # elif (columnTypes[column[0]] == str and columnValue != None):
                        #     columnValue = columnValue.encode(
                        #         'latin-1', 'ignore').decode('utf-8')

                        if (column[0] != "staging_id"):
                            insertRow += (columnValue, )

                    if("checkKeyExists" in etlObject and etlObject["checkKeyExists"]):
                        insertRow += (sysdate, )

                    insert_data.append(insertRow)

            # for c in sourceColumns:
            #     dpl = (diff_data[c].drop_duplicates(keep=False))
            #     if (dpl.shape[0] > 0):
            #         print(dpl)

            # # D E B U G
            if (len(delete_data) > 0 or len(insert_data) > 0):                
                debug = True
            
            if ("debugInfo" in etlObject):
                if ("deleted" not in etlObject["debugInfo"]):
                    etlObject["debugInfo"]["deleted"] = 0
                if ("inserted" not in etlObject["debugInfo"]):
                    etlObject["debugInfo"]["inserted"] = 0
                
                etlObject["debugInfo"]["deleted"] += len(delete_data)
                etlObject["debugInfo"]["inserted"] += len(insert_data)

            if (len(diff_data) > 0 and "dataTiggers" in etlObject):
                for trigger in etlObject["dataTiggers"]:
                    trg = importlib.import_module("etl-objects." + etlObject["desc"] + "." + trigger) 
                    trg.execute(sourceConnection=sourceConnection, destinationConnection=destinationConnection, diff_data=diff_data)
            
            if ("oltpDataTiggers" in etlObject):
                for trigger in etlObject["oltpDataTiggers"]:
                    trg = importlib.import_module("etl-objects." + etlObject["desc"] + "." + trigger) 
                    trg.execute(sourceConnection=sourceConnection, destinationConnection=destinationConnection, diff_data=oltp_data)
                
            if ("destinationTable" in etlObject):
                deleteCursor.executemany(None, delete_data)
                insertCursor.executemany(None, insert_data)

            

            if("crtKeyType" in etlObject and etlObject["crtKeyType"] == "numeric"):
                etlObject["crtKey"] = str(oltp_data[etlObject["keyField"]].values[-1])
            elif("crtKeyType" in etlObject and etlObject["crtKeyType"] == "string"):
                etlObject["crtKey"] = str(oltp_data[etlObject["keyField"]].values[-1])    
            elif("crtKeyType" in etlObject and etlObject["crtKeyType"] == "date"):
                etlObject["crtKey"] = str(pd.Timestamp(oltp_data[etlObject["keyField"]].values[-1]))
            else:
                etlObject["crtKey"] = str(pd.Timestamp(oltp_data[etlObject["keyField"]].values[-1]))

            file1 = open(path + "object.json", "w")
            file1.writelines(json.dumps(etlObject, indent=4))
            file1.close()

            destinationConnection.commit()
            delete_data = []
            insert_data = []
     
     # Stergere key sterse/disparute din OLTP
    if("checkKeyExists" in etlObject and etlObject["checkKeyExists"] and "resetDate" in etlObject):        
        if ("manualDeleteNotExisting" not in etlObject or etlObject["manualDeleteNotExisting"] == False):
            
        
            if ("deleteKeysNotExists" not in etlObject):
                etlObject["deleteKeysNotExists"]  = "update " + etlObject["destinationTable"]  + " set deleted_date = sysdate where check_key_exists < :checkKeyExists and deleted_date is null"        
            
            destinationCursor.execute(etlObject["deleteKeysNotExists"], {"checkKeyExists": dateutil.parser.parse(etlObject["resetDate"])})        
            destinationConnection.commit()
        

    checkSums(path, sourceConnection, destinationConnection)   

    # r = ",".join([str(v) for v in a])
# execute(path)

 