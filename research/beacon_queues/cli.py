import typer
from typing import Optional
from .main import query_epochs

app = typer.Typer(help="Beacon queues & EL backlog utilities")

@app.command()
def query(
    start_slot: int = typer.Option(..., help="Start slot (inclusive)"),
    end_slot: int = typer.Option(..., help="End slot (inclusive)"),
    interval: int = typer.Option(1, help="Slot step"),
    outfile: Optional[str] = typer.Option(None, "--outfile", "-o", help="CSV output path"),
    sleep: float = typer.Option(0.05, "--sleep", help="Seconds to sleep between slots (0 to disable)"),
):
    query_epochs(start_slot, end_slot, interval=interval, filename=outfile, sleep_between=sleep)

if __name__ == "__main__":
    app()
