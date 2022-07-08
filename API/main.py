from typing import List

import shutil
from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse

app = FastAPI()

@app.post("/uploadfiles/")
async def create_upload_files(files: List[UploadFile]):

    for file in files:
        with open("/opt/spark/work-dir/Data_Engineering/temporary_folder/{}".format(file.filename), "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    return {"filenames": [file.filename for file in files]}


@app.get("/")
async def main():
    content = """
            <style>
                 body {text-align: center;}
            </style>
            <body>
                    <br>
                    <br>
                    <br>
                <form action="/uploadfiles/" enctype="multipart/form-data" method="post" style="font-size: 24px;">
                    <input name="files" type="file" multiple style="font-size: 24px;">
                    <input type="submit" style="font-size: 24px;">
                </form>
            </body>
    """
    return HTMLResponse(content=content)