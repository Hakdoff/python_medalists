import os
from fastapi import UploadFile, HTTPException

async def validate_file(file: UploadFile):
    if file.content_type != 'text/csv':
        raise HTTPException(status_code=400, detail="Invalid file type. Only CSV files are allowed")
    if file.size > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds limit.")