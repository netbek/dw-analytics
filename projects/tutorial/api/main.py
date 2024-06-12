from litestar import get, Litestar
from package.database import get_client
from projects.tutorial.config.settings import get_settings

settings = get_settings()


@get("/")
async def index() -> dict[str, str]:
    return {"hello": "world"}


@get("/databases")
async def get_databases() -> list[dict[str, str]]:
    query = """
    select name, engine
    from system.databases
    """

    with get_client(settings.database.URL) as client:
        df = client.query_df(query)

    return df.to_dict(orient="records")


app = Litestar([index, get_databases])
