# 2024.01.08  19.00
import pandas as pd
import json as js
from fastapi import FastAPI

safety_df = pd.read_csv("snowflake_table_new.csv")

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
    safety_lev3_select = safety_df['EARNINGS_VIEW_LVL3'].dropna().unique().tolist()
    level3_dict = {'EARNINGS_VIEW_LVL3':safety_lev3_select}
    return js.loads(js.dumps(level3_dict)) 

@app.get('/query_data')
def query_data():
    safety_json = safety_df.to_json(orient ='records')
    return js.loads(safety_json)


