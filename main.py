from fastapi import FastAPI, BackgroundTasks
from pathlib import Path

from concatCsvs import DEFAULT_INPUT, DEFAULT_OUTPUT, concatCSVFolder

app = FastAPI(title="CSV Merger")


@app.get("/")
def root():
    return {"ok": True, "info": "POST /merge para concatenar"}


@app.post("/merge")
def merge(background_tasks: BackgroundTasks, input_folder: str = DEFAULT_INPUT, output_file: str = DEFAULT_OUTPUT, overwrite: bool = False):
    out_path = Path(output_file)
    if overwrite and out_path.exists():
        out_path.unlink()
    background_tasks.add_task(concatCSVFolder, input_folder, output_file)
    return {"message": "merge started", "input": input_folder, "output": output_file}
