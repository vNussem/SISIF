import cx_Oracle
import pyodbc

import pymssql  


from sqlalchemy.engine import create_engine


def getConnectionByName(name):
    if (name == "oracle"):
        return getORACLE()
    
    
    else:
        return None


def getORACLE():
    insis_tns = cx_Oracle.makedsn(
        'address', '1521', service_name='service_name')
    insis_connection = cx_Oracle.connect(
        user='user', password='password', dsn=insis_tns, encoding='iso-8859-1')
    return insis_connection    

def getOracleEngine():
    DIALECT = 'oracle'
    SQL_DRIVER = 'cx_oracle'
    USERNAME = 'username' #enter your username
    PASSWORD = 'password' #enter your password
    HOST = 'host' #enter the oracle db host url
    PORT = 1521 # enter the oracle port number
    SERVICE = 'service' # enter the oracle db service name
    ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + USERNAME + ':' + PASSWORD +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

    engine = create_engine(ENGINE_PATH_WIN_AUTH)
    return engine




def getSQLSever():
    
    # con = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=19sql01\ASRSQL;Database=contract;uid=interfacing_dwh;pwd=Asirom12Pr0d")
    con = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=server_address;Database=database;uid=user_name;pwd=password")
    return con

def getSQLSeverEngine():   

    DRIVER = "ODBC Driver 17 for SQL Server"
    USERNAME = "user"
    PSSWD = "password"
    # SERVERNAME = "19sql01"
    SERVERNAME = "servername"
    INSTANCENAME = ""
    DB = "contract"

    engine = create_engine(
        f"mssql+pyodbc://{USERNAME}:{PSSWD}@{SERVERNAME}{INSTANCENAME}/{DB}?driver={DRIVER}", fast_executemany=True
    )

    return engine


def getSQLSeverTrusted():
    con = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};Server=server_address;Database=database;Trusted_Connection=yes;")
    return con

