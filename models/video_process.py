import cv2
import sys
from keras.applications.vgg16 import VGG16
from keras.applications.vgg16 import preprocess_input as preprocess_input_vgg
from keras.preprocessing import image
import os
import numpy as np
from numpy import linalg as LA
sys.path.append("..")
from milvus.milvus_video import Collection_Video
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

def feature_extract(frame_list):
    model = VGGNet()   
    feats = []
    for img_path in frame_list:
        print("Generate the img feature vector:", img_path)
        norm_feat = model.vgg_extract_feat(img_path)
        feats.append(norm_feat)
    return feats

class VGGNet:
    def __init__(self):
        self.input_shape = (224, 224, 3)
        self.weight = 'imagenet'
        self.pooling = 'max'
        self.model_vgg = VGG16(weights=self.weight,input_shape=(self.input_shape[0], self.input_shape[1], 
                                            self.input_shape[2]), pooling=self.pooling, include_top=False)
        self.model_vgg.predict(np.zeros((1, 224, 224, 3)))

    def vgg_extract_feat(self, img_path):
        img = image.load_img(img_path, target_size=(self.input_shape[0], self.input_shape[1]))
        img = image.img_to_array(img)
        img = np.expand_dims(img, axis=0)
        img = preprocess_input_vgg(img)
        feat = self.model_vgg.predict(img)
        norm_feat = feat[0] / LA.norm(feat[0])
        return norm_feat.tolist()

def VideoProcess(video_path):
    save_path = "frame_res"      # Path to save frame
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    frame_list = []
    video_list = []
    time_video=[]

    if not video_path.endswith('.mp4'):
        print('video ko hop le')
    try:
        video_to_picture_path = os.path.join(save_path, video_path.split(".")[0])
        if not os.path.exists(video_to_picture_path):   # Create a folder corresponding to each video storage image
            os.makedirs(video_to_picture_path)
                
        src = os.path.join(f'/media/Data_Project/service/Data/Video/{video_path}')
        vid_cap = cv2.VideoCapture(src)    
        success, image = vid_cap.read()
        count = 0
        print(success)
        while success:
            vid_cap.set(cv2.CAP_PROP_POS_MSEC, 1 * 1000 * count)
            frame_path = video_to_picture_path + "/" + str(count) + ".jpg"
            #print(count)
            cv2.imwrite(frame_path, image)       # Addresses of stored images and naming of images
            frame_list.append(frame_path)
            time_video.append(count)
            video_list.append(video_path)
            success, image = vid_cap.read()
            count += 1
                
    except Exception as e:
        print("There has error:", e)
    print("\nThe video frame list:", frame_list)
    print("\nThe video list:", video_list)   
    vectors = feature_extract(frame_list)

    return vectors

def imageProcess(image_path):
    searchimg=image_path
    model=VGGNet()
    search_vector=model.vgg_extract_feat(searchimg)
    #print(type(search_vector))
    #print(search_vector)
    return search_vector

if __name__=="__main__":
    # path="Seoul.mp4"
    # con=Collection_Video()
    # a=VideoProcess(path)
    # #print(a)
    # print(len([a]))
    # idvideo="videotest"
    # #con.insert(idvideo,[a])
    path='2.jpg'
    a=imageProcess(path)
    print(a)











