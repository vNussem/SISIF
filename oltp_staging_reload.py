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


from datetime import datetime, time

import decimal



def execute(path, destinationConnection, reloadKeys, reloadQueryName=None, sourceConnection=None):

    etlObject = rff.getJson(path + "object.json")
    sysdate = datetime.now()
    # print(sysdate)

    if (sourceConnection == None):
        sourceConnection = dbConn.getConnectionByName(etlObject["sourceConnection"])
    else:
        sourceConnection = dbConn.getConnectionByName(sourceConnection)
    

    fetchsize = etlObject["fetchsize"]
    pk = etlObject["pk"]
    
    query = None
    if (reloadQueryName):
        query = rff.getText(path + reloadQueryName).format(str(reloadKeys).strip("[]").replace(",)", ")"))
    else:
        query = rff.getText(
            path + etlObject["query_reload"]).format(str(reloadKeys).strip("[]").replace(",)", ")"))
        
    sorceCursor = sourceConnection.cursor()

    sorceCursor.execute(query)

    columnTypes = {"staging_id": None}

    sourceColumns = []
    columns = ["staging_id"]
    for column in sorceCursor.description:
        columns.append(column[0].lower())
        sourceColumns.append(column[0].lower())
        columnTypes[column[0].lower()] = column[1]

    query = "select " + ",".join(columns) + " from " + \
        etlObject["destinationTable"] + \
            " where (" + ",".join(pk) + \
        ") in ({0}) and deleted_date is null"

    query_check_key_exists = "update " + \
        etlObject["destinationTable"] + \
        " set check_key_exists = :checkKeyExists " + \
        " where (" + ",".join(pk) + \
        ") in ({0}) and deleted_date is null"

    destinationCursor = destinationConnection.cursor()
    destinationCursor.prepare(query)

    deleteCursor = destinationConnection.cursor()
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


    insertCursor = destinationConnection.cursor()
    insertCursor.prepare("insert into " + etlObject["destinationTable"] + " (" +
                         insertColumns + ") values (" + values + ")")

    while True:
        pks = []
        rows = sorceCursor.fetchmany(fetchsize)

        if (not rows):
            break

        oltp_data = pd.DataFrame.from_records(rows, columns=sourceColumns)


        for column in columns:
            if (column != "staging_id" and columnTypes[column] == str):
                oltp_data[column] = oltp_data[column]\
                    .str.encode(encoding="ascii", errors="replace")\
                    .str.decode('utf-8', errors="ignore")

                oltp_data[column] = pd.DataFrame(
                    np.where(oltp_data[column].str.len() == 0, None, oltp_data[column]))


        for index, row in oltp_data.iterrows():
            pkValue = ()
            for pkColumn in pk:
                if (type(row[pkColumn.lower()]) == pd._libs.tslibs.timestamps.Timestamp):
                    pkValue += ("**to_date('" + str(row[pkColumn.lower()]) + "', 'yyyy-mm-dd hh24:mi:ss. ')**",)
                else:
                    pkValue += (row[pkColumn.lower()],)
            pks.append(pkValue)
            

        if (len(pks) == 0):
            return
            
        query_format = ",".join([str(p).replace(",)", ")").replace('"**', '').replace('**"', '') for p in pks])
        
        # Verificare key sterse/disparute din OLTP
        if("checkKeyExists" in etlObject and etlObject["checkKeyExists"]):
            destinationCursor.execute(query_check_key_exists.format(query_format), {"checkKeyExists": sysdate})
        #End verificare


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
                dest_data.loc[~dest_data[column].isnull(), column] = dest_data.loc[~dest_data[column].isnull(
                )][column].apply(str).apply(decimal.Decimal)

        # end to do

        checkColuns = []
        for ck in sourceColumns:
            if (not "ignoreChangesForColumns" in etlObject or ck not in etlObject["ignoreChangesForColumns"]):
                checkColuns.append(ck)

        # diff_data = pd.concat([oltp_data, dest_data]).drop_duplicates(subset=sourceColumns, keep=False)
        diff_data = pd.concat([oltp_data, dest_data]).drop_duplicates(subset=checkColuns, keep=False)

        

        delete_data = []
        insert_data = []

        for index, diff_row in diff_data.iterrows():
            stagingId = diff_row["staging_id"]
            if (not np.isnan(stagingId) and int(stagingId) > 0):
                delete_data.append((int(stagingId), ))
                
            else:
                insertRow = ()
                for column in diff_row.iteritems():
                    columnValue = column[1]

                    if (type(columnValue) == np.int64):
                        columnValue = int(columnValue)
                    if (pd.isna(columnValue)):
                        columnValue = None
                    if (columnTypes[column[0]] == datetime and columnValue != None):
                        columnValue = str(columnValue)
                    if (column[0] != "staging_id"):
                        insertRow += (columnValue, )

                if("checkKeyExists" in etlObject and etlObject["checkKeyExists"]):
                    insertRow += (sysdate, )

                insert_data.append(insertRow)

        
        # D E B U G
        if (len(delete_data) > 0 or len(insert_data) > 0):
            debug = True

        if (len(diff_data) > 0 and "dataTiggers" in etlObject):
            for trigger in etlObject["dataTiggers"]:
                trg = importlib.import_module("etl-objects." + etlObject["desc"] + "." + trigger) 
                trg.execute(sourceConnection=sourceConnection, destinationConnection=destinationConnection, diff_data=diff_data)
        
        deleteCursor.executemany(None, delete_data)
        insertCursor.executemany(None, insert_data)
    
    # destinationConnection.commit()
    # delete_data = []
    # insert_data = []



