from litestar import get, Litestar
from package.database import get_client
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

    with get_client(CLICKHOUSE_URL) as client:
        df = client.query_df(query)

    return df.to_dict(orient="records")


app = Litestar([index, get_databases])
