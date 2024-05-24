from fastapi import FastAPI
from package.database import Database
from projects.tutorial.constants import CLICKHOUSE_URL

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
        df = db.client.query_df(query)

    return df.to_dict(orient="records")
