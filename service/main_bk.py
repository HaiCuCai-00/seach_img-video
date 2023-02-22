from typing import Optional, List
from fastapi import FastAPI, File, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pyrsistent import v
import uvicorn
import shutil
import io
from starlette.responses import HTMLResponse
from fastapi.responses import FileResponse
import json
import sys
sys.path.append("..")
from milvus.milvus_image import Collection_Image
from milvus.milvus_video import Collection_Video
sys.path.append("..")
from models.image_process import ProcessImage
from models.video_process import VideoProcess 
from models.video_process import imageProcess

app=FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Data(BaseModel):
    table_name:Optional[str]=None
    ids:List[str]=None

class Update_Student(BaseModel):
    name:Optional[str]=None
    age:Optional[int]=None
    year: Optional[str]=None
img=Collection_Image()
con=Collection_Video()


@app.post("/insert_image")
def Insert_img(file :UploadFile=File(...),
        meta: Optional[str]=Form("null")):
     
    try:
        with open(f'Data/Image/{file.filename}', 'wb') as fileup:
            shutil.copyfileobj(file.file, fileup)
        name=f'Data/Image/{file.filename}'
        meta = json.loads(meta) or {}
        ids=meta["id"]
        if ids=="":
            return {"status": False, "msg": "ID not be empty"}
        index=meta["index"]
        table_name= index+"_image"
        ids=meta["id"]
        vector=ProcessImage(name)
        img.insert(table_name,[vector],ids)
        return {"status": True, "msg": "Successfully insert into database"}
    except Exception as e:
        print("error: ",e)
        return {"status": False, "msg": "Fail to insert into database"}

@app.post('/search_image')
def Search_img(file :UploadFile=File(...),
    meta:Optional[str]=Form("null")):

    if file.filename=="":
        return {"status": False , "msg": "No input file image   "}
    if file.filename.endswith(".jpg")==False and file.filename.endswith(".png") ==False:
        return{"status": False, "msg": "Input file image"}
    with open(f'Data/Search/{file.filename}', 'wb') as fileup:
        shutil.copyfileobj(file.file, fileup)

    name=f'Data/Search/{file.filename}'
    vector=ProcessImage(name)
    meta = json.loads(meta) or {}
    topk=meta["topk"]
    index=meta["index"]
    table_name= index+"_image"

    if index == "":
        return {"status": False , "msg": "index not be empty"}
    if meta == "":
        return {"status": False , "msg": "meta not be empty"}

    image_path=img.search_vectors(table_name,vector,int(topk))
    return image_path
    #return HTMLResponse(content=content)
    #return StreamingResponse(image_path, media_type="image/png")
    #return FileResponse(str(image_path))

@app.post("/delete")
def delete(param:Data):
    try:
        index=param.table_name+"_image"
        ids=param.ids
        if index == "":
            return {"status": False, "msg": "Index not be empty!"}
        if ids == "":
            return {"status": False, "msg": "Ids not be empty!"}
        for id in ids:
            img.drop(index,id)
        return {"status": True, "msg": "Successfully delete entity in Milvus and MySQL!"}
    except Exception as e:
        return {"status": False, "msg": "Faild to delete entity in Milvus and MySQL!"}

@app.post('/insert_video')
def Insert_video(file :UploadFile= File(...),
    meta: Optional[str]=Form("null")):
    
    try:
        if file.filename =="":
            return {"status": False, "msg": "Not found file video"}
        if file.filename.endswith(".mp4") == False or file.filename.endswith(".avi"):
            return{"status": False, "msg": "Input file mp4"}

        with open(f'Data/Video/{file.filename}', 'wb') as fileup:
            shutil.copyfileobj(file.file, fileup)

        name=f'{file.filename}'
        meta=json.loads(meta) or {}
        print(name)
        a=VideoProcess(name)
        idvideo=meta["id"]
        if idvideo=="":
            return {"status": False, "msg":"ID not be empty"}
        idvideo=meta["id"]
        index=meta["index"]
        table_name= index+"_video"
        con.insert(table_name,idvideo,[a])
        
        return {"status": True, "msg": "Successfully insert into database"}
    except Exception as e:
        return {"status": False , "msg": "Error insert file"}


@app.post('/search_video')
def Search_video(file: UploadFile = File(...),
        meta: Optional[str]=Form("null")):
   
    #try:
        meta=json.loads(meta) or {}
        if file.filename =="":
            return {"status": False, "msg": "Not found file"}
        if file.filename.endswith(".jpg")==False and file.filename.endswith(".png") ==False:
            return{"status": False, "msg": "Input file image"}
            
        with open(f'Data/Search/{file.filename}','wb') as fileup:
            shutil.copyfileobj(file.file, fileup)
        
        name=f'Data/Search/{file.filename}'
        vector=imageProcess(name)
        index=meta["index"]
        table_name= index+"_video"
        vector=imageProcess(name)
        path_video=con.search_vectors(table_name,[vector],1)
        return path_video
    #except Exception as e:
         #return {"status": False , "msg": "Error search video"}

@app.post("/delete_video")
def delete(param:Data):
    try:
        index=param.table_name+"_video"
        ids=param.ids
        con.drop(index,ids)
        return {"status": False, "msg": "Successfully delete entity in Milvus and MySQL!"}
    except Exception as e:
        return {"status": False, "msg": "Faild to delete entity in Milvus and MySQL!"}

if __name__ =="__main__":
    uvicorn.run(app, debug=True, host="0.0.0.0", port=1718)
