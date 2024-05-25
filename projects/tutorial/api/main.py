from litestar import get, Litestar
from package.database import Database
from projects.tutorial.constants import CLICKHOUSE_URL


@get("/")
async def index() -> dict[str, str]:
    return {"hello": "world"}


@get("/databases")
async def get_databases() -> list[dict[str, str]]:
    query = """
    select name, engine
    from system.databases
    """

    with Database(CLICKHOUSE_URL) as db:
        df = db.client.query_df(query)

    return df.to_dict(orient="records")


app = Litestar([index, get_databases])
