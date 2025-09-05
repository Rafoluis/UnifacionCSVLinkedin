from fastapi import FastAPI, BackgroundTasks
from pathlib import Path

from DockerScripts.UnificacionCSVLinkedin.mergeCSV import DEFAULT_INPUT, DEFAULT_OUTPUT, UnificarCsvCarpeta

app = FastAPI(title="CSV Merger")


@app.get("/")
def root():
    return {"ok": True, "info": "POST /merge para concatenar"}


@app.post("/merge")
def merge(background_tasks: BackgroundTasks, input_folder: str = DEFAULT_INPUT, output_file: str = DEFAULT_OUTPUT, overwrite: bool = False):
    out_path = Path(output_file)

    if overwrite and out_path.exists() and out_path.is_file():
        out_path.unlink()

    background_tasks.add_task(UnificarCsvCarpeta, input_folder, output_file)

    return {
        "message": "merge started",
        "input": str(input_folder),
        "output_dir": str(output_file),  # directorio base
    }
