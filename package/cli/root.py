import rich.console
import typer

app = typer.Typer(add_completion=False)


@app.callback()
def main():
    app.console = rich.console.Console()
