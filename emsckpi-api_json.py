# 2024.01.08  10.00
import pandas as pd 
import json as js
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from fastapi import FastAPI

# MSSQL SQLAlchemy connection
azure_sql_conn = URL.create("mssql+pyodbc", host="emsckpi-api.database.windows.net", port="1433", database="emsckpi-api",
    query={"driver":"ODBC Driver 17 for SQL Server", "authentication":"ActiveDirectoryIntegrated", "Encrypt":"yes", "TrustServerCertificate": "yes"}) 

engine = create_engine(azure_sql_conn)
sqlalch_conn = engine.connect()

safety_df = pd.read_sql("SELECT TOP 10 * FROM dbo.snowflake_table", sqlalch_conn)
#print(safety_df.head)

#sqlalch_conn.close()
#engine.dispose()

app = FastAPI()

@app.get('/')
def default_route():
    return {"message":"serving data from snowflake_table"}

@app.get('/level1')
def query_data1():
    safety_lev1_select = safety_df['EARNINGS_VIEW_LVL1'].unique().tolist()
    level1_dict = {'EARNINGS_VIEW_LVL1':safety_lev1_select}
    return js.loads(js.dumps(level1_dict)) 

@app.get('/level2')
def query_data2():
    safety_lev2_select = safety_df['EARNINGS_VIEW_LVL2'].unique().tolist()
    level2_dict = {'EARNINGS_VIEW_LVL2':safety_lev2_select}
    return js.loads(js.dumps(level2_dict)) 

@app.get('/level3')
def query_data3():
    safety_lev3_select = safety_df['EARNINGS_VIEW_LVL3'].unique().tolist()
    level3_dict = {'EARNINGS_VIEW_LVL3':safety_lev3_select}
    return js.loads(js.dumps(level3_dict)) 

@app.get('/query_data')
def query_data():
    query1_data = pd.read_sql("SELECT TOP 50 * FROM dbo.snowflake_table", sqlalch_conn)
    query1_json = query1_data.to_json(orient ='records')
    return js.loads(query1_json)
    #return js.loads(js.dumps(query1_json))

#import nest_asyncio
#import uvicorn
#if __name__ == "__main__":
#    nest_asyncio.apply()
#    uvicorn.run(api_server)


