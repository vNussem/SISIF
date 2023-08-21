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


def clean(path):

    etlObject = rff.getJson(path + "object.json")

    # stagingConnection = dbConn.getStaging()    
    stagingConnection = dbConn.getConnectionByName(
        etlObject["destinationConnection"])

    

    if ("autoclean" not in etlObject or etlObject["autoclean"] == False):
        return

    noOfRowsCursor = stagingConnection.cursor()
    noOfRowsQuery = None
    if ("cleanQuery" in etlObject):
        noOfRowsQuery = rff.getText(path + etlObject["cleanQuery"])
    else:        
        noOfRowsQuery = "\
            select {0} \
            from {1} \
            where (\
                    dwh_inserted is not null \
                and dwh_deleted  is not null \
            ) \
            or ( \
                dwh_inserted  is     null and dwh_deleted  is     null \
            and inserted_date is not null and deleted_date is not null \
            )\
        "

    noOfRows = noOfRowsCursor.execute(noOfRowsQuery.format("count(*)", etlObject["destinationTable"])).fetchall()[0][0]

    scopeCursor = stagingConnection.cursor()
    scopeCursor.execute(noOfRowsQuery.format("staging_id", etlObject["destinationTable"]))
    
    deleteCursor = stagingConnection.cursor()
    deleteCursor.prepare("\
        delete from {0} \
        where staging_id = :stagingId \
    ".format(etlObject["destinationTable"]))

    fetchsize = etlObject["fetchsize"]
    with tqdm(total=noOfRows, desc="cleaning " + etlObject["desc"] if "desc" in etlObject else "No desc") as pbar:
        while noOfRows > 0:
            stagingIds = []
            rows = scopeCursor.fetchmany(fetchsize)

            scopeData = pd.DataFrame.from_records(rows, columns=["staging_id"])
            
            for index, row in scopeData.iterrows():
                stagingIds.append((int(row["staging_id"]),))
                noOfRows -= 1
                pbar.update(1)

            deleteCursor.executemany(None, stagingIds)
            stagingConnection.commit()

    stagingConnection.close()

# D E B U G
if False:
    etlPath = "python\data-sync\etl-objects\\"
    task = "insis-policy"

    print(task)
    clean(etlPath + "\\" + task + "\\")