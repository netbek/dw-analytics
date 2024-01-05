from fastapi import FastAPI
from package.database import Database
from projects.tutorial.constants import CLICKHOUSE_URL

import pandas as pd

app = FastAPI()


@app.get("/")
def root():
    return {"hello": "world"}


@app.get("/databases")
def read_databases():
    query = """
    select name, engine
    from system.databases
    """

    with Database(CLICKHOUSE_URL) as db:
        result = db.execute(query)
        df = pd.DataFrame(result.all())

    return df.to_dict(orient="records")
