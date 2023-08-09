from typing import Union

from fastapi import FastAPI,BackgroundTasks
from fastapi.responses import FileResponse

from connect import session
from utility import generate_report,convert_to_datetime
import uuid
import os
app = FastAPI()



def csv_generator_background(csv_id: str):
    print("generating a CSV file...")

    current_time = "2023-01-25 18:13:22.47922 UTC"
    current_time = convert_to_datetime(current_time)
    generate_report(session,current_time,csv_id,1,24,168)

@app.get("/")
def read_root():
    return "welcome to the API services"

@app.get("/trigger_report")
async def trigger_report(background_tasks: BackgroundTasks):
    csv_id = uuid.uuid4()
    background_tasks.add_task(csv_generator_background, csv_id)
    return {"report_id": csv_id}


@app.get("/get_report")
async def get_report(report_id: str):
    folder_path = "./results"
    csv_filename = os.path.join(folder_path, f"{report_id}_data.csv")

    if not os.path.exists(csv_filename):
        return "Running"
    else:
        return FileResponse(csv_filename, media_type="text/csv", filename="generated report.csv")




