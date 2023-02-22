import requests
import uuid
import os
import towhee
import sys
sys.path.append("..")
from models.image_process import ProcessImage
from milvus.milvus_image import Collection_Image
img=Collection_Image()


data_dir = "./VOCdevkit/VOCdevkit/VOC2012" # You can replace this to your local directory of image folders
pattern = "*.jpg"
# url='http://123.25.30.4:7979/insert_image'
subfolders = [os.path.join(data_dir, x) for x in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, x))]
headers = {'Content-Type': 'application/json'}
steps = len(subfolders)
step = 1
for i in range(1,22):
    print(i)
    for sub_dir in subfolders:
        img_pattern = os.path.join(sub_dir, pattern)
        paths = towhee.glob(img_pattern).to_list()
        # files = {'media': open(paths, 'rb')}
        # requests.post(url, files=files)
        
        
        print(type(paths))
        for path in paths:
            print(path)
            ids=uuid.uuid1().hex
            vector=ProcessImage(path)
            img.insert("XO6_image",[vector],ids)

        
