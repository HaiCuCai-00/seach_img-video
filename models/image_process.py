from tokenize import Pointfloat
import matplotlib.pyplot as plt
import cv2
import torch
from loguru import logger
from torchvision import datasets, transforms, models
from PIL import Image
import sys
sys.path.append("..")
from milvus.milvus_image import Collection_Image

def ProcessImage(image):
    try:
        model = torch.hub.load('pytorch/vision:v0.9.0', 'resnet18', pretrained=True)
            
    except Exception as e:
        logger.error("Failed to load model: {}".format(e))
    encoder = torch.nn.Sequential(*(list(model.children())[:-1]))
    encoder.eval()
    transform_ops = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

    #embeddings = [transform_ops(Image.open(x).convert('RGB')) for x in image]
    x=Image.open(image).convert('RGB')
    embeddings= [transform_ops(x)]
    embeddings = torch.stack(embeddings, dim=0)

    with torch.no_grad():
        embeddings = encoder(embeddings).squeeze().numpy()
    img=[embeddings.tolist()]
    return img

if __name__=="__main__":
    name='2.jpg'
    collection="DHVB_image"
    ids="123456"
    img= Collection_Image()
    a=ProcessImage(name)
    img.insert(collection,[a],ids)
    #img.search_vectors(a,5)
    
