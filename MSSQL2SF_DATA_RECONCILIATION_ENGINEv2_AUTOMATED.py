import numpy as np
import pandas as pd
from pandas import read_sql
import pandas._libs.testing as _testing
import pyodbc
from time import sleep
import snowflake.connector as sf
from snowflake.connector.pandas_tools import pd_writer, write_pandas
import datetime
from datetime import timedelta
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from Decryptor import getDecryptedPassword
import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL
import sys


print(f"Data Load script started: {str(datetime.datetime.now())}")
sys.stderr.flush()
sys.stdout.flush()

class sql_connection:
    @staticmethod
    def get_credentials(db_alias):
        cn_list = config_db.db_connection
        db_server = list(filter(lambda i : i['db_alias'] == db_alias, cn_list))
        if len(db_server) > 0 :
            cn = db_server[0]
            cred = db_credentials(cn['server_name'], cn['port'], cn['database_name'], cn['schema_name'], cn['user_name'],
                                 cn['password'], cn['protocol'], cn['account'], cn['authenticator'], cn['keypath'])
        else :
            cred = None

        return cred

class config_db:
    db_connection = \
[
    {'db_alias': 'db2_prod', 'database_name': 'CFCPEMS', 'server_name': 'lnxvdb2hq002.cinfin.com', 'port': '50090', 'protocol': 'TCPIP', 
     'schema_name': 'db2prd44', 'user_name': 'YAIL_ADMIN', 
     'password': b'gAAAAABjyYCtaoscMdd8D26vnHJDBFBvgffs39TTdSP1S_Jmz2b5_1Bb-cPpiY4yQJTBrXPi9YKqfNgMnsPOHWWnznL-Bogu_A==', 'protocol' : '',
     'account' : '', 'authenticator' : '', 'keypath' : ''},
    
    {'db_alias': 'db_stage','server_name': 'lnxvdb2hq014.cinfin.com', 'port':'50008' , 'database_name': 'CFCPACDM', 
     'schema_name':'STAGE', 'user_name': 'halsys', 'password': b'gAAAAABjb_Fhi1S-SfDbCNyUpySzdcb4mWVFoFcCvbedJKYcv5MKRt0kXfka63kBuyb4XjlRdjPDXhBmwLObfRJR5Yu_O8ZKuQ==', 'protocol' : '',
     'account' : '', 'authenticator' : '', 'keypath' : ''},

    {'db_alias' : 'db_yail_prod', 'server_name' : 'CORPSQLHQPCF085', 'port' : '4787', 'database_name' : 'HQ1New',
     'schema_name' : 'YAILStage', 'user_name' : 'YAIL_ADMIN',
     'password' : b'gAAAAABj2BXOQyGPN53ZXvg72aWRGLKbOozwW6wSLvt2NYs0Uv2fp6f00UwPs4S70tR_YOf9rTeP9wYKk6uupBu7A6rdELHbw1RerlZqhm0ErloQbZQv-O0=', 'protocol' : '',
     'account' : '', 'authenticator' : '', 'keypath' : ''},

    {'db_alias' : 'db_yail_dev', 'server_name' : 'CORPSQLHQTTC073', 'port' : '4758', 'database_name' : 'DEV1',
     'schema_name' : 'YAILStage', 'user_name' : 'YAIL_DEV_ADMIN',
     'password' : b'gAAAAABjmlqi_rNzICflvzmS6ykmik5Mm7EcQI3jNwhcN4B5TTkA8M_p22eTdMNUuU2H5nboFhgjEp3yIOrhmrMDpQCFT9k1sGD_2r6KHzNXPMueQDLtyvc=', 'protocol' : '',
     'account' : '', 'authenticator' : '', 'keypath' : ''},
    
    {'db_alias' : 'snowflake_local', 'server_name' : '', 'port' : '', 'database_name' : '', 'schema_name': '', 'user_name' : 'shane_winslow@cinfin.com', 'password' : '', 'protocol' : '',
     'account' : 'cinfin1.east-us-2.azure', 'authenticator' : 'externalbrowser', 'keypath': ''},
    
    {'db_alias' : 'snowflake', 'server_name' : '', 'port' : '', 'database_name' : '', 'schema_name': '', 'user_name' : 'CDWADMIN_RSA_PRD', 'password' : '', 'protocol' : '',
     'account' : 'cinfin1.east-us-2.azure', 'authenticator' : '', 'keypath' : "/home/cdwpy_admin/.ssh/rsa_key.p8"}
]

class db_credentials :
    def __init__(self, server_name = '', port = '', db_name = '', schema = '', user_name = '', password= '', protocol = '', account = '', authenticator = '', keypath = '') :
        self._server_name = server_name
        self._port = port
        self._db_name = db_name
        self._schema = schema
        self._user_name = user_name
        self._password = password
        self._protocol = protocol
        self._account = account
        self._authenticator = authenticator
        self._keypath = keypath
        
    @property
    def server_name(self) :
        return self._server_name

    @property
    def port(self) :
        return self._port

    @property
    def username(self) :
        return self._user_name

    @property
    def db_name(self) :
        return self._db_name

    @property
    def password(self) :
        return self._password

    @property
    def schema(self) :
        return self._schema
    
    @property
    def protocol(self) :
        return self._protocol
    
    @property
    def account(self) :
        return self._account
    
    @property
    def authenticator(self) :
        return self._authenticator
    
    @property
    def keypath(self) :
        return self._keypath

def decrypt_password(password):
    try:
        
        decrypted = getDecryptedPassword(password)
        return decrypted
    
    except Exception as ex:
        print(f"Error in decrypt_password, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        
    
#def getEncryptedSqlCn():
#    drv = "ODBC Driver 17 for SQL Server"
#    db_cred = sql_connection.get_credentials('db_yail_prod')
#    passwd = decrypt_password(db_cred.password)
#    cn_string = f'driver={drv};server={db_cred.server_name}\{db_cred.db_name},{db_cred.port};' \
#                          f'database={db_cred.schema};' \
#                          f'uid={db_cred.username};pwd={passwd.decode()}'
#    cnxn = pyodbc.connect(cn_string)
#    
#    return cnxn, cn_string

def getSQLConnectString(configs):
    try:
        drv = "ODBC Driver 17 for SQL Server"
        db_cred = sql_connection.get_credentials(configs[0])
        passwd = decrypt_password(db_cred.password)
        #cn_string = f'driver={drv};server={db_cred.server_name}\{db_cred.db_name},{db_cred.port};' \
        #                      f'database={db_cred.schema};' \
        #                      f'uid={db_cred.username};pwd={passwd.decode()}'
        #YAIL_ADMIN:Jim$pa$$w0rD09ser6#@CORPSQLHQPCF085\HQ1New, 4787/YAILStage?driver=ODBC Driver 17 for SQL Server    
        cn_string = f'{db_cred.username}:{passwd.decode()}@{db_cred.server_name}\{db_cred.db_name}, {db_cred.port}/{db_cred.schema}?driver={drv}'

        print(cn_string)

        return cn_string
    
    except Exception as ex:
        print(f"Error in getSQLConnectString, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
    

def getSQLAlchemyEngine(configs):
    try:
        connect_string = getSQLConnectString(configs)
        #engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={connect_string}', fast_executemany=True)
        #engine = sqlalchemy.create_engine("mssql+pyodbc://YAIL_ADMIN:Jim$pa$$w0rD09ser6#@CORPSQLHQPCF085\HQ1New, 4787/YAILStage?driver=ODBC Driver 17 for SQL Server", fast_executemany=True)
        engine = sqlalchemy.create_engine(f"mssql+pyodbc://{connect_string}", fast_executemany=True)
        return engine
    
    except Exception as ex:
        print(f"Error in get_sf_connection_ext_browser, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        sys.exit() 

def get_sf_connection_ext_browser(db_cred):
    try:    
        return sf.connect(
                account=db_cred.account,
                user=db_cred.username,
                authenticator=db_cred.authenticator,
                database='SF_BIDM_WORK_PRD',
                schema='SF_SNOWFLAKE_SQLSERVER_FULL_LOADS',
                role='SF_CDW_WORK_APP_ROLE_PRD',
                warehouse='WH_DATAQUALITY_PRD'
            )
        
    except Exception as ex:
        print(f"Error in get_sf_connection_ext_browser, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        
def get_sf_connection_local_engine(db_cred):
    try:    
        engine = create_engine(URL(
            #'snowflake://{user}:@{account}/{database}/{schema}?authenticator={authenticator}&role={role}&warehouse={warehouse}'.format(
            account=db_cred.account,
            user=db_cred.username,
            authenticator=db_cred.authenticator,
            role = 'SF_CDW_WORK_APP_ROLE_PRD',
            warehouse='WH_DATAQUALITY_PRD',
            database='SF_BIDM_WORK_PRD',
            schema='SF_SNOWFLAKE_SQLSERVER_FULL_LOADS'
        ))
        
        print(engine)
        return engine
        
    except Exception as ex:
        print(f"Error in get_sf_connection_ext_browser, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        

#def getSQLCnxn():
#    cnxnSuccessful = 0
#    while cnxnSuccessful == 0:
#        try:
#            sqlCnxn, connect_string = getEncryptedSqlCn()
#            cursor = sqlCnxn.cursor()
#            cnxnSuccessful = 1
#        except pyodbc.Error as err:
#            #minutes = round(sleepTimeDurationOnError_Seconds/60, 2) 
#            print(f"Connection to YAIL database failed, see errormsg: {str(err)}")
#            #print(f"EmsEventConnector sleeping for {str(minutes)} minutes...")
#            print(f"...")
#            #sys.stderr.flush()
#            #sys.stdout.flush()
#            #time.sleep(sleepTimeDurationOnError_Seconds) 
#            sys.stderr.flush()
#            sys.stdout.flush()
#            
#    return sqlCnxn, connect_string, cursor
        
def get_snowflake_connection(configs):
    try:
        snowflake_ctx = ''
        sql_yail_alias, sf_suffix, sf_yail_alias, connection_type  = configs
        db_cred = sql_connection.get_credentials(sf_yail_alias)

        if connection_type == 1:
            #snowflake_ctx = get_sf_connection_ext_browser(db_cred)
            engine = get_sf_connection_local_engine(db_cred)
        else:
            #snowflake_ctx = get_server_connection(db_cred)
            engine = get_server_connection_engine(db_cred)

        #snowflake_cursor = snowflake_ctx.cursor()
        #snowflake_cursor.execute(f'USE ROLE SF_BIDM_WORK_APP_ROLE_{sf_suffix}') #Need to set role in appropriate environment to call sf stored procs
        #snowflake_cursor.execute(f'USE DATABASE SF_BIDM_WORK_{sf_suffix}')
        #snowflake_cursor.execute(f'USE WAREHOUSE WH_BIDM_{sf_suffix}')
        #snowflake_cursor.execute(f'USE SCHEMA SF_SNOWFLAKE_SQLSERVER_FULL_LOADS')
        #snowflake_cursor.execute('ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE')

        print(engine)
        print("Snowflake engine successfully created")

        return engine
        #return snowflake_cursor, snowflake_ctx
    
    except Exception as ex:
        print(f"Error in get_snowflake_connection, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        

def get_server_connection(db_cred):
    try:
        with open(db_cred.keypath, "rb") as key: #Need to grab this key from linux box
            p_key= serialization.load_pem_private_key(
                key.read(),
                #Set PRIVATE_KEY_PASSPHRASE environment variable in server environment
                password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

            ctx = sf.connect(
                user=db_cred.username,
                account=db_cred.account,
                private_key=pkb,
                #warehouse='WH_BIDM_PRD',
                #database='SF_BIDM_WORK_PRD',
                #schema='SF_EVENTDATA'
                )

            return ctx
    
    except Exception as ex:
        print(f"Error in get_server_connection, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        
def get_server_connection_engine(db_cred):
    try:
        with open(db_cred.keypath, "rb") as key: #Need to grab this key from linux box
            p_key= serialization.load_pem_private_key(
                key.read(),
                #Set PRIVATE_KEY_PASSPHRASE environment variable in server environment
                password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

            engine = create_engine(URL(
                    #'snowflake://{user}:@{account}/{database}/{schema}?private_key={private_key}&role={role}&warehouse={warehouse}'.format(
                    user=db_cred.username,
                    account=db_cred.account,
                    warehouse='WH_DATAQUALITY_PRD',
                    role='SF_CDW_WORK_APP_ROLE_PRD',
                    database='SF_BIDM_WORK_PRD',
                    schema='SF_SNOWFLAKE_SQLSERVER_FULL_LOADS'
                    ), 
                    connect_args={
                    'private_key': pkb,
                    },
                    )

            return engine

    except Exception as ex:
        print(f"Error in get_server_connection, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        

def check_existance_of_missingPKs_datasets(SQLengine):
    try:
        i = 0
        keep_missingPKs_datasets = np.array("")
        print(f"Checking if missingPKs tables exist in SQLServer...")
        sys.stderr.flush()
        sys.stdout.flush()

        sql = f"exec YAILStage.dbo.SP_CHECK_EXISTING_MISSINGPK_DATASETS"
        df = pd.read_sql_query(sql, SQLengine)
        for index, row in df.iterrows():
            answer = input(f"Replace {row['SCHEMA_NM']}.{row['SOURCE_NAME']} missing PKs comparison table? y or n?").strip()
            sys.stderr.flush()
            sys.stdout.flush()

            while answer.strip() != 'y' and answer.strip() != 'n':
                print("Please answer only with y or n...")
                answer = input(f"Replace {row['SCHEMA_NM']}.{row['SOURCE_NAME']} missing PKs comparison table? y or n?").strip()

            if answer == 'n':
                if i==0:
                    keep_missingPKs_datasets = np.array(f"{row['SCHEMA_NM']}.{row['SOURCE_NAME']}")
                else:
                    keep_missingPKs_datasets = np.append(keep_missingPKs_datasets, f"{row['SCHEMA_NM']}.{row['SOURCE_NAME']}")

            i+=1


        return keep_missingPKs_datasets
    
    except Exception as ex:
        print(f"Error in check_existance_of_missingPKs_datasets, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush() 
        

def get_snowflake_list_of_sources(sf_engine):
    try:
        source_table = ''
        success = 0
        batchsize = 10000
        #Add call to refresh view
        sql1 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_CFXML_TABLE_VIEW()"
        sql2 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_GET_SOURCE_NAMES_FOR_MULTI_COMPARE()"
        
        
        with sf_engine.connect() as snowflake_cursor:
            snowflake_cursor.execute('ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE')
            snowflake_cursor.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
            snowflake_cursor.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
        
            print(f"{sql1} - {str(datetime.datetime.now())}")
            msg = pd.read_sql(sql1, snowflake_cursor)
            success = 1
            print(str(msg))
            sys.stderr.flush()
            sys.stdout.flush()
        
            if success == 1:
                success = 0
                print(f"{sql2} - {str(datetime.datetime.now())}")
                sys.stderr.flush()
                sys.stdout.flush()
                source_table = pd.read_sql(sql2, snowflake_cursor)
                success = 1
        
        snowflake_cursor.close()
        return source_table, success
           
    except Exception as ex:
        print(f"Error in get_list_of_sources, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return source_table, success

def get_date_countcheck_boundaries(schema_sql, source_name, max_datetime, SQLengine):
    try:
        boundary_limits = ''
        upper_boundary_limit_date= ''
        pkAuditDatasourceTracking = ''
        success = 0
        
        with SQLengine.begin() as SQLCursor:
            sql = text(f"EXEC YAILStage.dbo.GetNextBoundaryForCountCheck_v2 @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}', @maxSnowflakeActivityDatetime='{max_datetime}'")
            print(sql.text)
            
            result = SQLCursor.execute(sql)
            boundaries = result.fetchone()
            pkAuditDatasourceTracking = boundaries['PK']
            lower_limit = boundaries['LowerLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_limit = boundaries['UpperLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_boundary_limit_datetime = boundaries['UpperLimitCountCheck']
            
        
            #sql2 = text(f"select top 1 LowerLimitCountCheck, UpperLimitCountCheck, PK from  \
            #            YAILStage.dbo.CountCheckBoundariesForDatasource where 1=1 and SourceSystem = '{source_name}'  \
            #            and SpecialSchema = '{schema_sql}'  \
	        #            order by PK desc;")
            #
            #result = SQLCursor.execute(sql2)
            #boundaries = result.fetchone()
            #pkAuditDatasourceTracking = boundaries['PK']
            #lower_limit = boundaries['LowerLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            #upper_limit = boundaries['UpperLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            #upper_boundary_limit_datetime = boundaries['UpperLimitCountCheck']
            success = 1
            
            print(f"Searching {source_name}  in Snowflake with boundaries between {lower_limit} and {upper_limit}")
            sys.stderr.flush()
            sys.stdout.flush()
            
            boundary_limits = lower_limit, upper_limit
        
        return boundary_limits, pkAuditDatasourceTracking, upper_boundary_limit_datetime, success
    
    except Exception as ex:
        print(f"Error in get_date_countcheck_boundaries, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return boundary_limits, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success 
    
    except Exception as ex:
        print(f"Error in get_date_countcheck_boundaries, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return boundary_limits, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success 

def get_hourly_count_boundaries(schema_sql, source_name, SQLengine):
    try:
        boundary_limits = ''
        upper_boundary_limit_date= ''
        pkAuditDatasourceTracking = ''
        success = 0
        
        with SQLengine.begin() as SQLCursor:
            sql = text(f"EXEC YAILStage.dbo.GetNextBoundaryForCountCheck @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
            result = SQLCursor.execute(sql)
            boundaries = result.fetchone()
            pkAuditDatasourceTracking = boundaries['PK']
            lower_limit = boundaries['LowerLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_limit = boundaries['UpperLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_boundary_limit_datetime = boundaries['UpperLimitCountCheck']
            
        
            #sql2 = text(f"select top 1 LowerLimitCountCheck, UpperLimitCountCheck, PK from  \
            #            YAILStage.dbo.CountCheckBoundariesForDatasource where 1=1 and SourceSystem = '{source_name}'  \
            #            and SpecialSchema = '{schema_sql}'  \
	        #            order by PK desc;")
            #
            #result = SQLCursor.execute(sql2)
            #boundaries = result.fetchone()
            #pkAuditDatasourceTracking = boundaries['PK']
            #lower_limit = boundaries['LowerLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            #upper_limit = boundaries['UpperLimitCountCheck'].strftime("%Y-%m-%d %H:%M:%S.%f")
            #upper_boundary_limit_datetime = boundaries['UpperLimitCountCheck']
            success = 1
            
            print(f"Searching {source_name}  in Snowflake with boundaries between {lower_limit} and {upper_limit}")
            sys.stderr.flush()
            sys.stdout.flush()
            
            boundary_limits = lower_limit, upper_limit
        
        return boundary_limits, pkAuditDatasourceTracking, upper_boundary_limit_datetime, success
    
    except Exception as ex:
        print(f"Error in get_date_countcheck_boundaries, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return boundary_limits, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success        

def get_datetime_boundaries(schema_sql, source_name, start_time, SQLengine):
    try:
        boundary_limits = ''
        upper_boundary_limit_datetime = ''
        pkAuditDatasourceTracking = ''
        success = 0
        
        with SQLengine.begin() as SQLCursor:
            sql = text(f"EXEC YAILStage.dbo.GetNextDatetimeBoundariesForDatasource @Datasource='{source_name}', @SpecialSchema='{schema_sql}', @Start_time = '{start_time}'")
            print(sql)
            SQLCursor.execute(sql)
        
            sql2 = text(f"select top 1 LowerActivityDatetimeLimit, UpperActivityDatetimeLimit, pkAuditDatasourceTracking from  \
                        YAILStage.dbo.Audit_Datasource_Tracking where 1=1 and SourceSystemName = '{source_name}'  \
                        and SpecialSchemaName = '{schema_sql}'  \
	                    order by pkAuditDatasourceTracking desc;")
            
            print(sql2)
            result = SQLCursor.execute(sql2)
            boundaries = result.fetchone()
            pkAuditDatasourceTracking = boundaries['pkAuditDatasourceTracking']
            lower_limit = boundaries['LowerActivityDatetimeLimit'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_limit = boundaries['UpperActivityDatetimeLimit'].strftime("%Y-%m-%d %H:%M:%S.%f")
            upper_boundary_limit_datetime = boundaries['UpperActivityDatetimeLimit']
            success = 1
            
            print(f"Searching {source_name}  in Snowflake with boundaries between {lower_limit} and {upper_limit}")
            sys.stderr.flush()
            sys.stdout.flush()
            
            boundary_limits = lower_limit, upper_limit
        
        return boundary_limits, pkAuditDatasourceTracking, upper_boundary_limit_datetime, success
    
    except Exception as ex:
        print(f"Error in get_datetime_boundaries, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return boundary_limits, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success
    
def save_target_dates_to_sqlserver(df, SQLengine, source_name, schema_sql):
    try:
        success = 0
        table_name = f'{source_name}_Datetime_Snowflake2SQL_Count_Mismatch'
        sql = text(f"EXEC dbo.CreateCountMismatchTableInSQL @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
        
        with SQLengine.connect() as SQLCursor:
            SQLCursor.execute(sql)
            success = 1
            print(f"{table_name} created in SQL Server")
                
            print("Appending data to table now")
            sys.stderr.flush()
            sys.stdout.flush()
            
            if df.values.shape[0] >= 1:
                success = 0
                df.to_sql(table_name, SQLCursor, schema=schema_sql, if_exists='append',index=False,chunksize=10000)
                success = 1
            
        return success  
        
    except Exception as ex:
        print(f"Error in save_target_dates_to_sqlserver, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
    
def save_target_hours_to_sqlserver(df, SQLengine, source_name, schema_sql):
    try:
        success = 0
        table_name = f'{source_name}_Hourly_Snowflake2SQL_Count_Mismatch'
        
        with SQLengine.connect() as SQLCursor:
            sql = text(f"EXEC dbo.CreateHourlyCountMismatchTableInSQL @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
            SQLCursor.execute(sql)
            print(sql)
            print(f"{table_name} created in SQL Server")
            success = 1
         
            if df.values.shape[0] >= 1: 
                success = 0  
                print("Appending data to table now")
                sys.stderr.flush()
                sys.stdout.flush()
            
                df.to_sql(table_name, SQLCursor, schema=schema_sql, if_exists='append',index=False,chunksize=10000)
                success = 1
                
        return success  
        
    except Exception as ex:
        print(f"Error in save_target_hours_to_sqlserver, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success

def get_snowflake_count_info (schema_sql, schema_snowflake, source_name, sf_engine, SQLengine, boundaries):
    try:
        success = 0
        batchsize = 10000
        row_count = 0
        cfxml_count = 0
        counts = ''
        start_time, end_time = boundaries
        i = 0
        iterator = 0
        #sql = f"CALL SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_GET_CFXML_INFO('{schema_snowflake}', '{source_name}')"
        #sql = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_DATETIME_PKFK_LIST_FOR_DATASOURCE('{schema_snowflake}', '{start_time}', '{end_time}')"
        sql1 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_DATETIME_PKFK_COUNTS_FOR_DATASOURCE('{schema_snowflake}', '{source_name}', '{start_time}', '{end_time}')"    
        sql2 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_SF_SQL_COUNTCOMPARE('{source_name}', '{schema_snowflake}')"
        sql3 = text(f"EXEC dbo.CreateCountMismatchTableInSQL @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
        sql4 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_SF_CHECKFORDUPES('{schema_snowflake}', '{source_name}')"
        print(f"{sql1} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        with sf_engine.connect() as snowflake_cursor:
            snowflake_cursor.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
            snowflake_cursor.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
            snowflake_response = pd.read_sql(sql4, snowflake_cursor)
            print(f"Creating Snowflake counts table for comparison - {str(datetime.datetime.now())}")  
            snowflake_counts = pd.read_sql(sql1, snowflake_cursor)
            #results = snowflake_cursor.execute(sql1).fetchone()
            print(f"Snowflake counts table finished - {str(datetime.datetime.now())}")
            sys.stderr.flush()
            sys.stdout.flush()
        
            print(f"Starting comparison between Snowflake and SQLServer- {str(datetime.datetime.now())}")
            print(f"{sql2}")
            
            df = pd.read_sql(sql2, snowflake_cursor)
            #snowflake_cursor.execute(sql2)
            print(f"Snowflake SQLServer count comparison finished - {str(datetime.datetime.now())}")
            sys.stderr.flush()
            sys.stdout.flush()
            success = 1
            
            if df.values.shape[0] < 1:
                with SQLengine.connect() as SQLCursor:
                    print("No count mismatches detected, creating empty table")
                    print(sql3)
                    SQLCursor.execute(sql3)
                    success = 1
                return success, row_count
            
            else:
                row_count += df.shape[0]
                print(f"Saving {row_count} rows to sql server")
                sys.stderr.flush()
                sys.stdout.flush()
                if success == 1:
                    success = save_target_dates_to_sqlserver(df, SQLengine, source_name, schema_sql)
                
        snowflake_cursor.close()
        return success, row_count
            
    except Exception as ex:
        print(f"Error in get_snowflake_count_info, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success

def get_cfxml_info (schema_sql, schema_snowflake, source_name, sf_engine, SQLengine, Dateframe, FullDateCheckFlag):
    try:
        success = 0
        batchsize = 250000
        row_count = 0
        cfxml_count = 0
        counts = ''
        #start_time, end_time = boundaries
        i = 0
        sf_table = f'SQL_{schema_sql}_{source_name}_PKFK_MISMATCH_LIST'
        sql1 = text(f"EXEC YAILStage.[dbo].[GetDatetimePKFKListForDatasource_withDedupeCheck_v2] @SourceSystem = '{source_name}', @SpecialSchema = '{schema_sql}', @Dateframe = '{Dateframe}', @FullDateCheckFlag = '{FullDateCheckFlag}'")
        print(f"{sql1} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        with SQLengine.begin() as SQLCursor:
            data = pd.read_sql(sql1, SQLCursor)
            print(f"{sql1} Stored Proc executed successfully")
            sys.stderr.flush()
            sys.stdout.flush()
            
            print("Loading data to Snowflake")
            sys.stderr.flush()
            sys.stdout.flush()
            
            with sf_engine.connect() as connection:
                if not data.empty:
                    df = pd.DataFrame(data, columns = ['PK', 'TABLENAME', 'TABLESCHEMA', 'YCDT'])
                    connection.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
                    connection.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
                    
                    #success, num_chunks, num_rows, output = write_pandas(
                    #    conn=snowflake_ctx,
                    #    df=df,
                    #    table_name=sf_table,
                    #    database=sf_db,
                    #    schema=sf_schema
                    #)
                    
                    #Add percentage complete logic here
                    if row_count < 1:
                       df.to_sql(sf_table.lower(), connection, index=False, if_exists='replace', method=pd_writer)
                    else:     
                        df.to_sql(sf_table.lower(), connection, index=False, if_exists='append', method=pd_writer)
                    row_count += df.shape[0]     
                    print(f"{row_count} rows loaded to SQL_{source_name.upper()}_COUNTS table")
                else:
                    print(f"0 rows to load for SQL_{source_name.upper()}_PKFK_MISMATCH_LIST table")
            connection.close()        
        success = 1
        
        #with sf_engine.connect() as snowflake_cursor:
        #    snowflake_cursor.execute(sql1)
        #    success = 1
        #
        #    while True:
        #        data = snowflake_cursor.fetchmany(batchsize)
#
        #        if not data:
        #            if iterator == 0:
        #               with SQLengine.begin() as SQLCursor:
        #                    SQLCursor.execute(sql2)
        #                    print(f"No data available - creating empty snowflake comparison table")
        #                    sys.stderr.flush()
        #                    sys.stdout.flush()
        #            break
        #        
        #        df = pd.DataFrame(data, columns=['PK', 'TABLENAME', 'TABLESCHEMA', 'YAILCompletionDateTime'])
        #        row_count += df.shape[0]
        #        for rows in df.TABLENAME:
        #            if rows == f'{source_name}_CFXML':
        #                cfxml_count += 1
#
        #        #Add percentage complete logic here
        #        print(f"Row count for {schema_sql}.{source_name} - {row_count}, {str(datetime.datetime.now())}")
        #        sys.stderr.flush()
        #        sys.stdout.flush()
        #        
        #        if success == 1: 
        #            success = save_cfxml_data_to_sqlserver(schema_sql, source_name, df, batchsize, iterator, SQLengine, success)
        #        else:
        #            return success
#
        #        iterator += 1
                
        #snowflake_cursor.close()    
        counts = row_count, cfxml_count
        return counts, success 
            
    except Exception as ex:
        print(f"Error in get_cfxml_info_sources, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
        
def save_snowflake_counts_to_sqlserver(schema_sql, source_name, df, batch_size, iterator, SQLengine, success):
    try:
        success = 0
        table_name = f"SF_{source_name}_Datetime_PKFK_Counts" 	#SF_ACE_FullPKLoad	
        starttime = datetime.datetime.now()
        sql = text(f"EXEC YAILStage.dbo.Create_Datetime_PKFK_Counts @source_name='{source_name}', @schema_name='{schema_sql}'")
            
        with SQLengine.begin() as SQLCursor:
            if iterator < 1: 
                #sql = text(f"EXEC YAILStage.dbo.Create_table_SF_PK_FullDataLoad @source_name='{source_name}', @schema_name='{schema_sql}'")
                SQLCursor.execute(sql)
                print(f"{table_name} created")
                print(f"SQL batch insert for {table_name} starting - {str(starttime)}")
                sys.stderr.flush()
                sys.stdout.flush()

            df.to_sql(table_name, SQLCursor, schema=schema_sql, if_exists='append',index=False,chunksize=10000)
            success = 1

        endtime = datetime.datetime.now()
        timerun = endtime - starttime
        print(f"SQL batch insert for {table_name} ended - {str(endtime)}, time run: {str(timerun.total_seconds())} seconds")
        sys.stderr.flush()
        sys.stdout.flush()
        
        return success
    
    except Exception as ex:
        print(f"Error in save_snowflake_counts_to_sqlserver, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success


def save_cfxml_data_to_sqlserver(schema_sql, source_name, df, batch_size, iterator, SQLengine, success):
    try:
        success = 0
        table_name = f"SF_{source_name}_Datetime_PKFK_Load" 	#SF_ACE_FullPKLoad	
        starttime = datetime.datetime.now()
            
        with SQLengine.begin() as SQLCursor:
            if iterator < 1: 
                #sql = text(f"EXEC YAILStage.dbo.Create_table_SF_PK_FullDataLoad @source_name='{source_name}', @schema_name='{schema_sql}'")
                sql = text(f"EXEC YAILStage.dbo.Create_Datetime_PKFK_Dataload @source_name='{source_name}', @schema_name='{schema_sql}'")
                SQLCursor.execute(sql)
                print(f"{table_name} created")
                print(f"SQL batch insert for {table_name} starting - {str(starttime)}")
                sys.stderr.flush()
                sys.stdout.flush()

            df.to_sql(table_name, SQLCursor, schema=schema_sql, if_exists='append',index=False,chunksize=10000)
            success = 1

        endtime = datetime.datetime.now()
        timerun = endtime - starttime
        print(f"SQL batch insert for {table_name} ended - {str(endtime)}, time run: {str(timerun.total_seconds())} seconds")
        sys.stderr.flush()
        sys.stdout.flush()
        
        return success
        
    except Exception as ex:
        print(f"Error in save_cfxml_data_to_sqlserver, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success

def save_mismatched_pk_to_sqlserver(df, SQLengine, source_name, schema_sql):
    try:
        success = 0
        table_name = f'{source_name}_PK_Snowflake2SQL_Mismatch'
        
        with SQLengine.connect() as SQLCursor:
            sql = text(f"EXEC [dbo].[CreatePKMismatchTableInSQL] @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
            SQLCursor.execute(sql)
            print(f"{table_name} created in SQL Server")
            success = 1
            
            if df.values.shape[0] >= 1: 
                success = 0  
                print("Appending data to table now")
                sys.stderr.flush()
                sys.stdout.flush()
            
                df.to_sql(table_name, SQLCursor, schema=schema_sql, if_exists='append',index=False,chunksize=10000)
                success = 1
                
        return success  
        
    except Exception as ex:
        print(f"Error in save_target_hours_to_sqlserver, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success

    
    
def get_sql_server_counts(source_name, schema_sql, SQLengine, boundaries, configs, sf_engine):
    try:
        success = 0
        rows = 0
        start_time, end_time = boundaries
        sf_table = f'SQL_{source_name}_COUNTS'
        sf_db = 'SF_BIDM_WORK_PRD'
        sf_schema = 'SF_SNOWFLAKE_SQLSERVER_FULL_LOADS' 
        sql = text(f"EXEC YAILStage.[dbo].[GetDatetimePKFKCountsForDatasource_withDedupeCheck] @SourceSystem = '{source_name}', @SpecialSchema = '{schema_sql}', @Starttime = '{start_time}', @Endtime = '{end_time}'")
        #sql = text(f"select TABLENAME, YCDT, COUNTS from YAILStage.{schema_sql}.SQL_{source_name}_DATETIME_PKFK_COUNTS")
        print (sql)
        print(f"Getting SQLServer counts comparison for {schema_sql}.{source_name} for timeframe between {start_time} and {end_time} - job started at {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        with SQLengine.begin() as SQLCursor:
            data = pd.read_sql(sql, SQLCursor)
            with sf_engine.connect() as connection:
                if not data.empty:
                    df = pd.DataFrame(data, columns = ['TABLENAME', 'YCDT', 'COUNTS'])
                    connection.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
                    connection.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
                    
                    if rows < 1:
                       df.to_sql(sf_table.lower(), connection, index=False, if_exists='replace', method=pd_writer)
                    else:     
                        df.to_sql(sf_table.lower(), connection, index=False, if_exists='append', method=pd_writer)
                    rows += df.shape[0]     
                    print(f"{rows} rows loaded to SQL_{source_name.upper()}_COUNTS table")
                else:
                    print(f"0 rows to load for SQL_{source_name.upper()}_COUNTS table")
            connection.close()        
        success = 1

        print(f"Datetime comparison finished - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        return success, sf_table
    
    except Exception as ex:
        print(f"Error in get_sql_server_counts, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
    
def compare_sql2snowflake(source_name, schema_sql, schema_snowflake, SQLengine, Dateframe, FullDateCheckFlag):
    try:
        
        success = 0
        batchsize = 10000
        row_count = 0
        cfxml_count = 0
        counts = ''
        #start_time, end_time = boundaries
        i = 0
        iterator = 0
        #sql = f"CALL SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_GET_CFXML_INFO('{schema_snowflake}', '{source_name}')"
        #sql = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_DATETIME_PKFK_LIST_FOR_DATASOURCE('{schema_snowflake}', '{start_time}', '{end_time}')"
        sql1 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_DATETIME_PKFK_LIST_FOR_DATASOURCE_V2('{source_name}', '{schema_snowflake}', '{Dateframe}', {FullDateCheckFlag});"    
        sql2 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_SF_SQL_PKCOMPARE('{source_name.upper()}', '{schema_snowflake}', '{schema_sql.upper()}')"
        sql3 = text(f"EXEC [dbo].[CreatePKMismatchTableInSQL] @SourceSystem='{source_name}', @SpecialSchema='{schema_sql}'")
        print(f"{sql1} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        with sf_engine.connect() as snowflake_cursor:
            snowflake_cursor.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
            snowflake_cursor.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
            print(f"Creating Snowflake counts table for comparison - {str(datetime.datetime.now())}")  
            snowflake_counts = pd.read_sql(sql1, snowflake_cursor)
            #results = snowflake_cursor.execute(sql1).fetchone()
            print(f"Snowflake counts table finished - {str(datetime.datetime.now())}")
            sys.stderr.flush()
            sys.stdout.flush()
        
            print(f"Starting comparison between Snowflake and SQLServer- {str(datetime.datetime.now())}")
            print(f"{sql2}")
            
            df = pd.read_sql(sql2, snowflake_cursor)
            #snowflake_cursor.execute(sql2)
            print(f"Snowflake SQLServer count comparison finished - {str(datetime.datetime.now())}")
            sys.stderr.flush()
            sys.stdout.flush()
            success = 1
            
            if df.values.shape[0] < 1:
                with SQLengine.connect() as SQLCursor:
                    print(f"0 rows to load for dateframe comparison")
                    SQLCursor.execute(sql3)
                    success = 1
                return success
            
            else:
                row_count += df.shape[0]
                print(f"Saving {row_count} rows for dateframe comparison to sql server")
                sys.stderr.flush()
                sys.stdout.flush()
                if success == 1:
                    success = save_mismatched_pk_to_sqlserver(df, SQLengine, source_name, schema_sql)
                
        snowflake_cursor.close()
        return success
            
    except Exception as ex:
        print(f"Error in compare_sql2snowflake, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
     
        #success = 0
        #start_time, end_time = boundaries
        ##snowflake_table = f"SF_{source_name}{schema_sql}_FullPKLoad"
        ##sql_table = f"{source_name}_CFXML"
        ##sql = text(f"EXEC YAILStage.[dbo].SP_SF_SQLServer_PKCompare @Source_Name = '{source_name}', @Schema_Name = '{schema_sql}'")
        #sql = text(f"EXEC YAILStage.[dbo].SP_SF_SQLServer_Datetime_PKFK_List_Compare @Source_Name = '{source_name}', @Schema_Name = '{schema_sql}', @Start_time = '{start_time}', @End_time = '{end_time}'")
        #print(f"Starting Snowflake/SQLServer comparison for {schema_sql}.{source_name} for timeframe between {start_time} and {end_time} - job started at {str(datetime.datetime.now())}")
        #sys.stderr.flush()
        #sys.stdout.flush()
#
        #with SQLengine.begin() as SQLCursor:
        #    SQLCursor.execute(sql)
        #    success = 1
#
        #print(f"Datetime comparison finished - {str(datetime.datetime.now())}")
        #sys.stderr.flush()
        #sys.stdout.flush()
        #
        #return success
    
def insert_to_AdditionalPKs_table(SQLengine):
    try:   
        sql = f"exec YAILStage.dbo.Get_Count_MissingPKs"
        source_missingPK_counts = pd.read_sql_query(sql, SQLengine)

        with SQLengine.connect() as connection:
            sql = ''
            for index, sources in source_missingPK_counts.iterrows():
                print(f"Pushing {sources['SCHEMA_NAME']}.{sources['SOURCE_NAME']} to AdditionalPKS table, counts = {sources['COUNTS']}")
                sql = text(f"exec [dbo].[SP_LOAD_ALL_PKS_FOR_SNOWFLAKE_PUSH] @Source_Name='{sources['SOURCE_NAME']}', @Schema_Name='{sources['SCHEMA_NAME']}'")
                connection.execute(sql)
                #connection.commit()
    
    except Exception as ex:
        print(f"Error in insert_to_AdditionalPKs_table, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        

def define_dataload_job():
    try:
        choice = 0
        print("Please select type of job to run...")

        while choice < 1 or choice > 4:
            print("Select \n"\
                    "1 - compare single source \n" \
                    "2 - compare all sources \n" \
                    "3 - compare timings \n" \
                    "4 - fix timings") 
            choice = input("Choice:")
            choice = int(choice)

            if choice > 4 or choice < 1:
                print("Please select between option 1 or 2")


        return choice
    
    except Exception as ex:
        print(f"Error in define_dataload_job, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        

def get_source_info(sf_engine):
    try:
        success = 0
        choice = -1
        max_index_count = -1
        batchsize = 10000
        sql = f"CALL SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_GET_SOURCE_NAMES()"
        
        with sf_engine.connect() as snowflake_cursor:
            snowflake_cursor.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE")
            snowflake_cursor.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
            snowflake_cursor.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
            source_table = pd.read_sql(sql, snowflake_cursor)
            print("Please select index # from dataset list...")
            print(source_table)

            for index, row in source_table.iterrows():
                max_index_count += 1

            while choice < 0 or choice > max_index_count:
                choice = input("Select dataset index #:")
                choice = int(choice)

                if choice < 0 or choice > max_index_count:
                    print(f"Please select between 0 to {max_index_count}")

            success = 1

        return choice, source_table, success
    
    except Exception as ex:
        print(f"Error in get_source_info, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return choice, source_table, success

def define_sql_alias(selection):
    try:
        switcher = {
            1: "db_yail_dev",
            2: "db_yail_qat",
            3: "db_yail_uat",
            4: "db_yail_prod"
        }

        return switcher.get(selection, "db_yail_dev")
    
    except Exception as ex:
        print(f"Error in define_sql_alias, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        sys.exit()    

def define_snowflake_suffix(selection):
    try:
        switcher = {
            1: "DEV",
            2: "QAT",
            3: "UAT",
            4: "PRD"
        }

        return switcher.get(selection, "SF_CDW_WORK_APP_ROLE_DEV")

    except Exception as ex:
        print(f"Error in define_snowflake_suffix, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
          

def get_connection_config():
    try:
        connection_type = 0
        env = 0
        snowflake_alias = ''
        snowflake_user = ''
        yail_db_alias = ''
        
        #Manually setting variables below to set for production
        connection_type = 1
        env = 4
        
        print("Please select which connection type you would like...")
        if connection_type == 0:
            while connection_type > 2 or connection_type < 1:
                print("Select from: \n" \
                    "1 - Local, \n" \
                    "2 - Server")
                connection_type = int(input("Choice:"))
        if env == 0:
            while env > 4 or env < 1:
                print("Please select which environment you would like...")
                print("Select from: \n" \
                    "1 - DEV, \n" \
                    "2 - QAT, \n" \
                    "3 - UAT, \n" \
                    "4 - PRD")
                env = int(input("Choice:"))
            
        yail_db_alias = define_sql_alias(env)
        snowflake_suffix = define_snowflake_suffix(env)
        
        if connection_type == 1:
            snowflake_alias = "snowflake_local"
        
        else:
            snowflake_alias = "snowflake"
                
        return yail_db_alias, snowflake_suffix, snowflake_alias, connection_type
    
    except Exception as ex:
        print(f"Error in get_connection_config, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()


def screen_for_dupes(source_name, schema_sql, SQLengine):
    try:
        success = 1
        Dupe_table = f'{source_name}_Dupes'
        True_missing = f'{source_name}_TrueMissingPKs'
        sql = text(f"EXEC YAILStage.dbo.SP_SCREEN_FOR_DUPES @SourceName = '{source_name}', @SchemaName = '{schema_sql}'")
        print(f"Checking for dupes for {schema_sql}.{source_name} - tables being created {Dupe_table} and {True_missing} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()

        with SQLengine.begin() as SQLCursor:
            SQLCursor.execute(sql)
            success = 2
            
        print(f"Tables created {Dupe_table}, {True_missing} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        return success
        
    except Exception as ex:
        print(f"Error in screen_for_dupes, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success


def ping_snowflake(snowflake_cursor, configs):
    try:
        success = 0
        #result = ''
        print("Checking Snowflake Connection")
        sys.stderr.flush()
        sys.stdout.flush()
        
        snowflake_cursor.execute("select 1=1")
        data = snowflake_cursor.fetchone()
        if data[0]:
            success = 1
            print("Connection stable")
            sys.stderr.flush()
            sys.stdout.flush()
        
            return snowflake_cursor, success
        else:
            success = 1
            print("Connection lost, re-establishing connection")
            sys.stderr.flush()
            sys.stdout.flush()
            
            snowflake_cursor, snowflake_ctx = get_snowflake_connection(configs)
            return snowflake_cursor, success
        
    except Exception as ex:
        print(f"Error in ping_snowflake, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return snowflake_cursor, success
        
def modify_sf_cnx(configs):
    try: 
        sql_yail_alias, sf_suffix, sf_yail_alias, connection_type  = configs
        db_cred = sql_connection.get_credentials(sf_yail_alias)

        engine = create_engine(URL(
            account=db_cred.account,
            user=db_cred.username,
            authenticator=db_cred.authenticator,
            role = 'SF_DEDUPING_ROLE_PRD',
            warehouse='WH_YAIL_TST',
            database='SF_HIST_YAILDB_PRD',
            schema='AUDIT_TIMINGS'
        ))

        return engine
    
    except Exception as ex:
        print(f"Error in modify_sf_cnx, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush() 

def getMaxSnowflakeActivityTimeForDatasource(schema_sql, source_name, SQLengine):
    try:
        success = 0
        max_datetime = ''
        with SQLengine.begin() as SQLCursor:
            sql = text(f"select cast(concat(substring(convert(varchar, max(ActivityPointInTime), 120), 1, 14), '00:00') as datetime2) as max_date \
                            from YAIL.dbo._SnowflakeActivity (NOLOCK) where 1=1 and SourceSystemName = '{source_name}' \
                            and SpecialSchemaName = '{schema_sql}' \
                            and AppliedStatus = 1 \
                            and CFXMLS > 0")
            
            result = SQLCursor.execute(sql)
            boundaries = result.fetchone()
            max_datetime = boundaries['max_date']
            max_datetime_string = max_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            success = 1
            
            print(f"{schema_sql}.{source_name} max ActivityPointInTime in Snowflake = {max_datetime_string}")
            sys.stderr.flush()
            sys.stdout.flush()
        
        return max_datetime, success
    
    except Exception as ex:
        print(f"Error in getMaxSnowflakeActivityTimeForDatasource, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush() 
        return max_datetime, success   
        
def update_target_datetime_tables(sourceSystemName, schemasql, Dateframe, counts, FullDateCheckFlag, SQLengine):
    try:
        success = 0
        
        row_count, cfxml_count = counts
        
        with SQLengine.begin() as SQLCursor:
            if FullDateCheckFlag == 1:
                sql = text(f"update YAILStage.{schemasql}.{sourceSystemName}_PKFK_Comparison_Target_Datetimes \
                                set Analysis_Complete = 1, Run_Completion_Datetime = getdate() \
                                where 1=1 \
                                and cast(Start_time as varchar(500)) like ''+substring(convert(varchar, '{Dateframe}', 120), 1, 10)+'%'")
                #print(sql)            
            else:    
                sql = text(f"update YAILStage.{schemasql}.{sourceSystemName}_PKFK_Comparison_Target_Datetimes \
                                set Analysis_Complete = 1, Run_Completion_Datetime = getdate() \
                                where 1=1 \
                                and convert(varchar, Start_time, 120) like '{Dateframe}%'")
                #print(sql)
            #Row_Count = {row_count}, CFXML_Count = {cfxml_count}
            SQLCursor.execute(sql)
            success = 1
            
            print(f"{schemasql}.{sourceSystemName} marked as successful in target datetime table for date {Dateframe}")
            sys.stderr.flush()
            sys.stdout.flush()
        
        return success
                
    except Exception as ex:
        print(f"Error in update_target_datetime_tables, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success    
        

def reconcile_timings(SQLengine, configs):
    try:
        rows = 0
        check = 1
        sql = text(f"EXEC YAILStage.dbo.StartCleanTimingsAfterDedupe")
        sql2 = text(f"EXEC YAILStage.dbo.GetNextBatchToDeleteFromTimings")
        sf_table = 'REMOVE_EVENTS_FROM_TIMINGS'
        df = ''
        
        sf_engine = modify_sf_cnx(configs)
        #print(f"Compiling identified duped events for Timings reconciliation to table YAILStage.dbo.TimingsEventsToBeDeleted")
        #print(f"Please wait, this may take a few minutes... Start time {str(datetime.datetime.now())}")
        #sys.stderr.flush()
        #sys.stdout.flush()
#
        #with SQLengine.begin() as SQLCursor:
        #    result = SQLCursor.execute(sql)
        #    for row in result:
        #        check = row.Status
        #
        if check == 1:
            print(f"YAILStage.dbo.TimingsEventsToBeDeleted table refreshed - End time {str(datetime.datetime.now())}")
            print("Modifying Snowflake connection to store to SF_HIST_YAILDB space")
            with SQLengine.begin() as SQLCursor:
                with sf_engine.connect() as connection:
                    connection.execute("USE ROLE SF_DEDUPING_ROLE_PRD")
                    connection.execute("USE WAREHOUSE WH_YAIL_TST")
                    connection.execute("USE DATABASE SF_HIST_YAILDB_PRD")
                    connection.execute("USE SCHEMA TIMINGS_CLEANUP")
                    
                    while True:
                        data = pd.read_sql(sql2, SQLCursor)
                        if data.empty:
                            break
                        df = pd.DataFrame(data, columns = ['PK', 'YAIL_COMPLETION_DATETIME', 'YAILCFXMLUUID', 'EVENT_TRANSACTION_CATEGORY', 'EVENT_LOG_FOREIGN_KEY', 'source_system_name'])
                        rows += df.shape[0]

                        #Add percentage complete logic here
                        if rows <= 10000:
                            df.to_sql(sf_table.lower(), connection, index=False, if_exists='replace', method=pd_writer)
                        else:     
                            df.to_sql(sf_table.lower(), connection, index=False, if_exists='append', method=pd_writer)     
                        
                        print(f"{rows} events loaded to TIMINGS_CLEANUP.REMOVE_EVENTS_FROM_TIMINGS in Snowflake")
                
                
    
    except Exception as ex:
        print(f"Error in reconcile_timings, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        
def get_max_upper_boundaries_for_count_check(source_name, schema_sql, SQLengine):
    try:
        success = 0
        upper_count_check_datetime = ''
        with SQLengine.begin() as SQLCursor:
            sql = text(f"select max(UpperLimitCountCheck) as max_date from YAILStage.dbo.CountCheckBoundariesForDatasource (nolock) \
                        where 1=1 and SourceSystem = '{source_name}'  \
                        and SpecialSchema = '{schema_sql}' \
                        and SuccessFlag = 1")
            
            result = SQLCursor.execute(sql)
            boundaries = result.fetchone()
            if boundaries['max_date'] is None:
                upper_count_check_datetime = datetime.datetime.min
            else:
                upper_count_check_datetime = boundaries['max_date']
            upper_count_check_datetime_string = upper_count_check_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            success = 1
            
            print(f"{schema_sql}.{source_name} max Count Check Boundary in SQLServer = {upper_count_check_datetime_string}")
            sys.stderr.flush()
            sys.stdout.flush()
        
        return upper_count_check_datetime, success
        
    except Exception as ex:
        print(f"Error in get_boundaries_for_count_check, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success  
    
def update_boundary_count_check_table(sourceSystemName, schemasql, pkAuditDatasourceTracking, SQLengine):
    try:
        success = 0
        
        #row_count, cfxml_count = counts
        #, Row_Count = {row_count}, CFXML_Count = {cfxml_count}
        
        with SQLengine.begin() as SQLCursor:
            sql = text(f"update YAILStage.dbo.CountCheckBoundariesForDatasource \
                            set SuccessFlag = 1 \
                            where 1=1 \
                            and PK = '{pkAuditDatasourceTracking}' \
                            and SourceSystem = '{sourceSystemName}' \
                            and SpecialSchema = '{schemasql}'")
            
            #RowsCount = {row_count}, CFXMLCount = {cfxml_count}
            SQLCursor.execute(sql)
            success = 1
            
            print(f"{schemasql}.{sourceSystemName} marked as successful in  table for pk = {pkAuditDatasourceTracking}")
            sys.stderr.flush()
            sys.stdout.flush()
        
        return success
                
    except Exception as ex:
        print(f"Error in update_audit_datasource_tracking, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
       
def get_target_datetimes_for_pkfk_comparison(source_name, schema_name, max_datetime, SQLengine):
    try:
        success = 0
        df = ''
        
        with SQLengine.begin() as SQLCursor:
            sql = text(f"EXEC dbo.CompilePKFKLoadTargetDatetimes_v2 @SourceSystem = '{source_name}', @SpecialSchema = '{schema_name}', @maxSnowflakeActivityDatetime = '{max_datetime}'")
            #results = SQLCursor.execute(sql)
            print(sql)
            data = pd.read_sql(sql, SQLCursor)
            if not data.empty:
                df = pd.DataFrame(data, columns = ['Dateframe', 'FullDateCheck'])
             
        success = 1 
                        
        return df, success
    
    except Exception as ex:
        print(f"Error in get_target_datetimes_for_pkfk_comparison, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success 

def get_current_upper_boundary(source_name, schema_sql, SQLengine):
    try:
        success = 0
        upper_boundary_limit_datetime = ''
        with SQLengine.begin() as SQLCursor:
            sql = text(f"select max(UpperActivityDatetimeLimit) as max_date from  \
                        YAILStage.dbo.Audit_Datasource_Tracking where 1=1 and SourceSystemName = '{source_name}'  \
                        and SpecialSchemaName = '{schema_sql}' \
                        and Successful_Run = 1")
            
            result = SQLCursor.execute(sql)
            boundaries = result.fetchone()
            if boundaries['max_date'] is None:
                upper_boundary_limit_datetime = datetime.datetime.min
            else:
                upper_boundary_limit_datetime = boundaries['max_date']
            upper_boundary_limit_datetime_string = upper_boundary_limit_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
            success = 1
            
            print(f"{schema_sql}.{source_name} max Audit Upper Boundary in Snowflake = {upper_boundary_limit_datetime_string}")
            sys.stderr.flush()
            sys.stdout.flush()
        
        return upper_boundary_limit_datetime, success
    
    except Exception as ex:
        print(f"Error in get_current_upper_boundary, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success  

def run_snowflake_hourlycount_comparison(schema_sql, schema_sf, source_name, sf_engine, SQLengine):
    try:
        success = 0
        batchsize = 10000
        row_count = 0
        cfxml_count = 0
        counts = ''
        i = 0
        iterator = 0
        sql1 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_CREATE_HOURLY_PKFK_MISMATCH_COUNTS_FOR_DATASOURCE('{source_name}', '{schema_sf}')"    
        sql2 = f"CALL SF_BIDM_WORK_PRD.SF_SNOWFLAKE_SQLSERVER_FULL_LOADS.SP_SF_SQL_HOURLY_COUNTCOMPARE('{source_name.upper()}', '{schema_sf}')"
        print(f"{sql1} - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        #while success == 0:
        #    snowflake_cursor, success = ping_snowflake(snowflake_cursor, configs)
        
        with sf_engine.connect() as snowflake_cursor:
            snowflake_cursor.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
            snowflake_cursor.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
            snowflake_cursor.execute(sql1)
            print(f"Snowflake counts table finished - {str(datetime.datetime.now())}")
        
            print(f"Starting comparison between Snowflake and SQLServer")
            print(f"{sql2}")
            df = pd.read_sql(sql2, snowflake_cursor)
            print(f"Snowflake SQLServer Count Comparison Finished")
        
            row_count += df.shape[0]
            print(f"Saving {row_count} rows to sql server")
            sys.stderr.flush()
            sys.stdout.flush()
                
            success = save_target_hours_to_sqlserver(df, SQLengine, source_name, schema_sql)
                        
        snowflake_cursor.close()
        return success
            
    except Exception as ex:
        print(f"Error in run_snowflake_hourlycount_comparison, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success

def compile_sql_hourly_mismatched(schema_sql, schema_sf, source_name, sf_engine, SQLengine):
    try:
        success = 0
        rows = 0
        sf_table = f'SQL_{source_name}_HOURLY_MISMATCH_COUNTS'
        sf_db = 'SF_BIDM_WORK_PRD'
        sf_schema = 'SF_SNOWFLAKE_SQLSERVER_FULL_LOADS' 
        sql = text(f"EXEC YAILStage.dbo.GetHourlyCountChecks_withDedupeCheck @SourceSystem = '{source_name}', @SpecialSchema = '{schema_sql}'")
        #sql = text(f"select TABLENAME, YCDT, COUNTS from {schema_sql}.SQL_{source_name}_HOURLY_PKFK_COUNTS (nolock)")
        print(f"Compiling hourly counts for dates where mismatches occurred for {schema_sql}.{source_name} - job started at {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        with SQLengine.begin() as SQLCursor:
            print (sql)
            data = pd.read_sql(sql, SQLCursor)
            with sf_engine.connect() as connection:
                connection.execute('USE ROLE SF_CDW_WORK_APP_ROLE_PRD')
                connection.execute('USE WAREHOUSE WH_DATAQUALITY_PRD')
                if not data.empty:
                    df = pd.DataFrame(data, columns = ['TABLENAME', 'YCDT', 'COUNTS'])
                    
                    #Add percentage complete logic here
                    if rows < 1:
                       df.to_sql(sf_table.lower(), connection, index=False, if_exists='replace', method=pd_writer)
                    else:     
                        df.to_sql(sf_table.lower(), connection, index=False, if_exists='append', method=pd_writer)
                    rows += df.shape[0]     
                    print(f"{rows} rows loaded to SQL_{source_name.upper()}_HOURLY_MISMATCH_COUNTS table")
                else:
                    print(f"0 rows to load for SQL_{source_name.upper()}_HOURLY_MISMATCH_COUNTS table")
                    
            connection.close()        
        success = 1

        print(f"Datetime comparison finished - {str(datetime.datetime.now())}")
        sys.stderr.flush()
        sys.stdout.flush()
        
        return success, sf_table
    
    except Exception as ex:
        print(f"Error in compile_sql_hourly_mismatched, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success
    
def check_for_jobs_to_rerun(source_name, schema_sql, SQLengine):
    try:
        rerunFlag = 0
        success = 0
        with SQLengine.begin() as SQLCursor:
            sql = text(f"select count(1) as rerunCounts from YAILStage.dbo.CountCheckBoundariesForDatasource (nolock) \
                        where 1=1 and SourceSystem = '{source_name}'  \
                        and SpecialSchema = '{schema_sql}' \
                        and RetryJob = 1")
            
            result = SQLCursor.execute(sql)
            rerunCounts = result.fetchone()
            if rerunCounts['rerunCounts'] > 0:
                rerunCounts = rerunCounts['rerunCounts']
                rerunFlag = 1
                print(f"{schema_sql}.{source_name} has {rerunCounts} jobs to rerun")
            else:
                print(f"{schema_sql}.{source_name} has 0 jobs to rerun")
            
            success = 1
            
            sys.stderr.flush()
            sys.stdout.flush()
        
        return rerunFlag, success
    
    except Exception as ex:
        print(f"Error in get_current_upper_boundary, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()
        return success  

def close_connections(SQLengine, sf_engine):
    SQLengine.dispose()
    sf_engine.dispose()
    
def multi_dataset_comparison(main_variables):
    try:
        configs, SQLengine, sf_engine, max_datetime, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success, target_datetimes, i, mismatch_count, rerun_jobs, jobs_available = main_variables

        for index, rows in source_table.iterrows():
            if success == 1:
                max_datetime, success = getMaxSnowflakeActivityTimeForDatasource(rows['schema_sql'], rows['source_system_name'], SQLengine)

            if success == 1:
                upper_count_check_datetime, success = get_max_upper_boundaries_for_count_check(rows['source_system_name'], rows['schema_sql'], SQLengine)

            while upper_count_check_datetime < max_datetime and max_datetime != '':
                jobs_available = 1

                if success == 1: 
                    boundaries, pkAuditDatasourceTracking, upper_count_check_datetime, success = get_date_countcheck_boundaries(rows['schema_sql'], rows['source_system_name'], max_datetime, SQLengine)

                if success == 1:
                    success, sf_table = get_sql_server_counts(rows['source_system_name'], rows['schema_sql'], SQLengine, boundaries, configs, sf_engine)

                if success == 1:
                    success, mismatch_count = get_snowflake_count_info(rows['schema_sql'], rows['schema_snowflake'], rows['source_system_name'], sf_engine, SQLengine, boundaries)

                if success == 1 and mismatch_count > 0:
                    success, sf_table = compile_sql_hourly_mismatched(rows['schema_sql'], rows['schema_snowflake'], rows['source_system_name'], sf_engine, SQLengine)

                    if success == 1:
                        success = run_snowflake_hourlycount_comparison(rows['schema_sql'], rows['schema_snowflake'], rows['source_system_name'], sf_engine, SQLengine)

                if success == 1:
                    success = update_boundary_count_check_table(rows['source_system_name'], rows['schema_sql'], pkAuditDatasourceTracking, SQLengine)
            
            if max_datetime != '':
                target_datetimes, success = get_target_datetimes_for_pkfk_comparison(rows['source_system_name'], rows['schema_sql'], max_datetime, SQLengine)
                if isinstance(target_datetimes, pd.DataFrame):
                    for index, row in target_datetimes.iterrows():

                        Dateframe = row['Dateframe']
                        FullDateCheckFlag = row['FullDateCheck']

                        if success == 1:
                            counts, success = get_cfxml_info(rows['schema_sql'], rows['schema_snowflake'], rows['source_system_name'], sf_engine, SQLengine, Dateframe, FullDateCheckFlag)

                        if success == 1:
                            success = compare_sql2snowflake(rows['source_system_name'], rows['schema_sql'], rows['schema_snowflake'], SQLengine, Dateframe, FullDateCheckFlag)

                        if success == 1:
                            success = update_target_datetime_tables(rows['source_system_name'], rows['schema_sql'], Dateframe, counts, FullDateCheckFlag, SQLengine)

        if jobs_available == 0:
            minute = 5
            seconds = minute * 60
            
            print(f"No jobs to check, sleeping for {minute} minutes...")
            sys.stderr.flush()
            sys.stdout.flush()
           
            sleep(seconds)
                
    except Exception as ex:
        print(f"Error in multi_dataset_comparison, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()            

#MAIN LINE
try: 
    configs = get_connection_config()
    SQLengine = getSQLAlchemyEngine(configs)
    sf_engine = get_snowflake_connection(configs)
    endJob = 0
    max_datetime = ''
    upper_boundary_limit_datetime = datetime.datetime.min
    pkAuditDatasourceTracking = ''
    success = 0
    source_table = ''
    target_datetimes = ''
    mismatch_count = 0
    rerun_jobs = 0
    i = 0
    jobs_available = 0
    
    source_table, success = get_snowflake_list_of_sources(sf_engine)
    
    main_variables = configs, SQLengine, sf_engine, max_datetime, upper_boundary_limit_datetime, pkAuditDatasourceTracking, success, target_datetimes, i, mismatch_count, rerun_jobs, jobs_available

    while 1==1 and isinstance(source_table, pd.DataFrame):

        multi_dataset_comparison(main_variables)    
    
except Exception as ex:
        print(f"Error in Main line, see errormsg: {str(ex)}")
        sys.stderr.flush()
        sys.stdout.flush()