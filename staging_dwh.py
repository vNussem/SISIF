
import pandas as pd

import db_connections as conn
from tqdm import tqdm
import numbers
import numpy
import numpy as np


import read_from_file as rff
from datetime import datetime, time
import importlib

import json

def execute(path):


    etlObject = rff.getJson(path + "object.json")
    sysdate = datetime.now()

    # print(etlObject)
    stagingConn = None
    if ("stgConnection" in etlObject):
        stagingConn = conn.getConnectionByName(etlObject["stgConnection"])
    else:
        stagingConn = conn.getStaging()

    dwhConnection = None
    if ("dwhConnection" in etlObject):
        dwhConnection = conn.getConnectionByName(etlObject["dwhConnection"])
    else:
        dwhConnection = conn.getDWH()

    noOfRowsCursor = stagingConn.cursor()
    query = etlObject["noOfRows"]
    noOfRows = noOfRowsCursor.execute(query).fetchall()[0][0]

    query = rff.getText(path + etlObject["query"])
    stagingCursor = stagingConn.cursor()
    stagingCursor.execute(query)

    fetchsize = etlObject["fetchsize"]
    sourceColumns = []
    for column in stagingCursor.description:        
        sourceColumns.append(column[0].lower())        

    with tqdm(total=noOfRows, desc=etlObject["desc"] if "desc" in etlObject else "No desc") as pbar:
        while noOfRows > 0:            

            dwhData = []

            rows = stagingCursor.fetchmany(fetchsize)
            stagingData = pd.DataFrame.from_records(rows, columns=sourceColumns)

            for processRows in etlObject["processRows"]:
                    trg = importlib.import_module("etl-objects-dwh." + etlObject["desc"] + "." + processRows) 
                    trg.execute(stagingData, dwhData)

            insert_data = []
            staging_ids = []
            for index, r in stagingData.iterrows():                
                staging_ids.append((r["staging_id"],))

            insertColumns = None
            insertValues = None
            for r in dwhData:
                if (insertColumns == None):
                    insertColumns = ""
                    insertValues = ""
                    for c in r.keys():
                        insertColumns += ("" if insertColumns == "" else ", ") + c
                        insertValues += ("" if insertValues == "" else ", ") + ":" + c


                insertRow = ()
                for c in r.keys():

                    columnValue = r[c]

                    if (type(columnValue) == numpy.int64):
                        columnValue = int(columnValue)                    
                    if (pd.isna(columnValue)):
                            columnValue = None
                    
                    insertRow += (columnValue, )

                insert_data.append(insertRow)

            

            

            delete_cursor = dwhConnection.cursor()
            deleteQuery = ("delete from " + etlObject["dwh_table"] + " where staging_id in ({0}) and helper = '" + etlObject["helper"] + "'").format(",".join([str(p[0]) for p in staging_ids]))
            delete_cursor.execute(deleteQuery)

            # insert_cursor = None
            if (len(dwhData) > 0):
                insertQuery = "insert into " + etlObject["dwh_table"] \
                    + "        (" + insertColumns + ")" \
                    + " values (" + insertValues  + ")"
                
                insert_cursor = dwhConnection.cursor()
                # insert_cursor.prepare(insertQuery)   
                
                if ("dwhConnection" in etlObject and (etlObject["dwhConnection"] == "actuariat")):
                    subqueries = ""
                    for r in insert_data:
                        insertQuery = "insert into " + etlObject["dwh_table"] + " with (TABLOCK) "\
                        + "        (" + insertColumns + ")"         
                        values = ""
                        colNo = 0
                        for c in r:                                                
                            if (c == None):
                                c = "null"
                            elif isinstance(c, datetime):
                                c = "convert(date, '" + str(c) + "', 102)"
                            elif isinstance(c, str):
                                c = "'" + c.replace("'", "") + "'"
                            values += ("select " if values == "" else ", ") + str(c) + ("" if subqueries != "" else " c" + str(colNo))

                            colNo += 1
                        
                        subqueries += ("" if subqueries == "" else " union all ") + values
                        # insertQuery += " values (" + values  + ")"
                    
                    insertQuery += " select * from (" + subqueries  + ") a "

                    insert_cursor.execute(insertQuery)
                else:
                    insert_cursor.prepare(insertQuery)
                    insert_cursor.executemany(None, insert_data)
            
            update_staging_cursor = stagingConn.cursor()
            # update_staging_cursor.prepare("update " + etlObject["dwh_table"] + "  set dwh_inserted = sysdate, dwh_deleted = decode(deleted_date, null, null, sysdate) where staging_id = :staging_id")
            update_staging_cursor.prepare("update " + etlObject["staging_table"] + "  set dwh_inserted = sysdate, dwh_deleted = decode(deleted_date, null, null, sysdate) where staging_id = :staging_id")
            update_staging_cursor.executemany(None, staging_ids)

            dwhConnection.commit()
            stagingConn.commit()

            noOfRows -= len(stagingData)
            pbar.update(len(stagingData))

    dwhConnection.close()
    stagingConn.close()            

    if ("integrityTrigger" in etlObject):
        integrityTrigger = importlib.import_module("etl-objects-dwh." + etlObject["desc"] + "." + etlObject["integrityTrigger"]["name"]) 
        integrityTrigger.execute(etlObject)

        file1 = open(path + "object.json", "w")
        file1.writelines(json.dumps(etlObject, indent=4))
        file1.close()

        summaryObject = rff.getJson(path + "/../summary.json")
        summaryObject[etlObject["desc"]] = etlObject["integrityTrigger"]
        
        file1 = open(path + "/../summary.json", "w")
        file1.writelines(json.dumps(summaryObject, indent=4))
        file1.close()    

    

# etlPath = "python\data-sync\etl-objects-dwh"
# task = "insis-prem_inst_fract"
# execute(etlPath + "\\" + task + "\\")