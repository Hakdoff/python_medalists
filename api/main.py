from fastapi import FastAPI, File, UploadFile, HTTPException
from utils import validate_file
from fastapi.responses import JSONResponse
from models import AggregatedStats
from motor.motor_asyncio import AsyncIOMotorClient
import shutil
import os
import logging

app = FastAPI()
ITEMS_PER_PAGE = 10

UPLOAD_DIR = "../storage/app/medalists/"

client = AsyncIOMotorClient("mongodb://localhost:27017") 
db = client["medalist_database1"]

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        await validate_file(file)
        file_location = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_location, "wb+") as file_object:
            shutil.copyfileobj(file.file, file_object)
        return JSONResponse(
            status_code =200,
            content={
                "message": f"File {file.filename} uploaded successfully"
                })
    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        raise HTTPException(
            status_code = 400,
            detail=f"Error uploading file: {str(e)}"
        )

@app.get("/aggregated_stats/event")
async def get_aggregated_stats(
    page: int = 1,
    limit: int = ITEMS_PER_PAGE,
):
    try:
        skip = (page - 1) * limit
        
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "discipline": "$discipline",
                        "event": "$event",
                        "event_date": "$medal_date"
                    },
                    "medalists": {
                        "$push": {
                            "name": "$name",
                            "medal_type": "$medal_type",
                            "gender": "$gender",
                            "country": "$country",
                            "country_code": "$country_code",
                            "nationality": "$nationality",
                            "medal_code": "$medal_code",
                            "medal_date": "$medal_date"
                        }
                    }
                }
            },
            {
                "$skip": skip
            },
            {
                "$limit": limit
            }
        ]
        
        count_pipeline = [
            {
                "$group": {
                    "_id": {
                        "discipline": "$discipline",
                        "event": "$event",
                        "event_date": "$medal_date"
                    }
                }
            }
        ]
        
        count_result = await db.medalists.aggregate(count_pipeline).to_list(None)
        total_items = len(count_result) if count_result else 0
        total_pages = -(-total_items // limit)
        
        data = await db.medalists.aggregate(pipeline).to_list(limit)
        
        response_data = []
        for item in data:
            response_data.append({
                "discipline": item["_id"]["discipline"],
                "event": item["_id"]["event"],
                "event_date": item["_id"]["event_date"],
                "medalists": item["medalists"]
            })
        
        return {
            "data": response_data,
            "paginate": {
                "current_page": page,
                "total_pages": total_pages,
                "next_page": f"/aggregated_stats/event?page={page + 1}" if page < total_pages else None,
                "previous_page": f"/aggregated_stats/event?page={page - 1}" if page > 1 else None
            }
        }
    
    except Exception as e:
        logging.error(f"Error in aggregation: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving aggregated stats: {str(e)}"
        )
