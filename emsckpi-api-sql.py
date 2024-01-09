# 2024.01.09  10.00
import pandas as pd 
import json as js
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from fastapi import FastAPI

# MSSQL SQLAlchemy connection
azure_sql_conn = URL.create("mssql+pyodbc", host="emsckpi-api.database.windows.net", port="1433", database="emsckpi-api",
    query={"driver":"ODBC Driver 17 for SQL Server", "authentication":"ActiveDirectoryIntegrated", "Encrypt":"yes", "TrustServerCertificate": "yes"}) 

def getsql_data(sql):
    engine = create_engine(azure_sql_conn)
    sqlalch_conn = engine.connect()
    safety_df = pd.read_sql(sql, sqlalch_conn)
    sqlalch_conn.close()
    engine.dispose()
    return safety_df

app = FastAPI()

@app.get('/')
def default_route():
    return {"message":"serving data from snowflake_table"}

@app.get('/level1')
def query_data1():
    safety1_df = getsql_data("SELECT * FROM dbo.snowflake_table")
    safety_lev1_select = safety1_df['EARNINGS_VIEW_LVL1'].unique().tolist()
    level1_dict = {'EARNINGS_VIEW_LVL1':safety_lev1_select}
    return js.loads(js.dumps(level1_dict)) 

@app.get('/level2')
def query_data2():
    safety2_df = getsql_data("SELECT * FROM dbo.snowflake_table")
    safety_lev2_select = safety2_df['EARNINGS_VIEW_LVL2'].unique().tolist()
    level2_dict = {'EARNINGS_VIEW_LVL2':safety_lev2_select}
    return js.loads(js.dumps(level2_dict)) 

@app.get('/level3')
def query_data3():
    safety3_df = getsql_data("SELECT * FROM dbo.snowflake_table")
    safety_lev3_select = safety3_df['EARNINGS_VIEW_LVL3'].dropna().unique().tolist()
    level3_dict = {'EARNINGS_VIEW_LVL3':safety_lev3_select}
    return js.loads(js.dumps(level3_dict)) 

@app.get('/query_data')
def query_data():
    safety_df = getsql_data("SELECT * FROM dbo.snowflake_table")
    safety_json = safety_df.to_json(orient ='records')
    return js.loads(safety_json)

       




