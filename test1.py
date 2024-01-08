# 2024.01.08  10.00
import pandas as pd 
import json as js
from fastapi import FastAPI

app = FastAPI()

@app.get('/')
def default_route():
    return {"message":"TEST serving data from snowflake_table"}
