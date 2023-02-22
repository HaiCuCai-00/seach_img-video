from pymilvus import connections, DataType, FieldSchema, CollectionSchema, Collection, utility
from pymilvus.orm import schema
from loguru import logger
import sys
import psycopg2


class Collection_Image():
    def __init__(self,host='127.0.0.1',port=19530):
        self.host=host
        self.port=port
       
        self.conn = psycopg2.connect(host='localhost', port='5438', user='postgres', password='postgres')
        self.cursor = self.conn.cursor()
        
    def set_collection(self,collection_name):
        try:
            collection=Collection(collection_name)
        except Exception as e:
            logger.error("failed to load data to milvus: {}".format(e))
            sys.exit(1)
    def has_collection(self,collection_name):
        try:
            return utility.has_collection(collection_name)
        except Exception as e:
            logger.error("Failed to load data to Milvus: {}".format(e))
            sys.exit(1)

    def create_collection(self,collection_name):
        try:
            connections.connect(host=self.host, port=self.port)
            print(collection_name)
            field1 = FieldSchema(
                name="id",
                dtype=DataType.INT64,
                descrition="int64",
                is_primary=True,
                auto_id=True,
            )
            field2 = FieldSchema(
                name="embedding",
                dtype=DataType.FLOAT_VECTOR,
                descrition="float vector",
                dim=512,
                is_primary=False,
            )
            schema=CollectionSchema(
                fields=[field1,field2],
                description="Collection image description",
            )
            collection= Collection(name=collection_name, schema=schema)
            collection.load()
            logger.debug("Create Milvus collection: {}".format(collection_name))
            
            #return "OK"
        except Exception as e:
            logger.error("Failed to load data to Milvus: {}".format(e))

        try:
            sql ="CREATE TABLE if not exists " + collection_name + " (milvus_id bigint, image_path text);"
            self.cursor.execute(sql)
            self.conn.commit()
            print("create postgres table successfully!")

        except Exception as e:
            print("cann't create a postgres table: ",e)

    def insert(self, collection_name,vectors,image_path):
        try:
            self.create_collection(collection_name)

            collection=Collection(collection_name)
            collection.load()
            mr=collection.insert(vectors)
            ids=mr.primary_keys
            logger.debug(
                "Insert vectors to Milvus in collection: {} with {} rows in collection: {}".format(
                    collection_name, len(vectors), collection_name
                )
            )
            #return ids
            print(ids[0])
        except Exception as e:
            logger.error("Failed to load data to Milvus: {}".format(e))
            sys.exit(1) 

        try:
            
            self.cursor.execute('INSERT INTO '+ collection_name +' (milvus_id ,image_path) values (%s,%s)' ,(ids[0],image_path))
            self.conn.commit()
            print('insert image done to psycopg2')
        except Exception as e:
            print("cann't not insert table", e)
    def create_index(self,collection_name):
        try:
            default_index = {
                "index_type": "IVF_FLAT",
                "metric_type": "L2",
                "params": {"nlist": 16384},
            }
            collection=Collection(collection_name)
            status = collection.create_index(
                field_name="embedding", index_params=default_index
            )
            if not status.code:
                logger.debug(
                    "Successfully create index in collection:{} with param:{}".format(
                        collection_name, default_index
                    )
                )
                return status
            else:
                raise Exception(status.message)
        except Exception as e:
            logger.error("Failed to create index: {}".format(e))
            sys.exit(1)
    def delete_collection(self,collection_name):
        try:
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            collection.drop()
            logger.debug("Successfully drop collection!")
            return "ok"
        except Exception as e:
            logger.error("Failed to drop collection: {}".format(e))
            sys.exit(1)
    
    def search_vectors(self,collection_name, vectors, top_k):
        try:
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            search_params={"metric_type": "L2", "params": {"nprobe": 10}}
            res=collection.search(
                vectors,
                anns_field="embedding",
                param=search_params,
                limit=top_k ,
            )
            logger.debug("Successfully search in collection: {}".format(res))
            #return res
        except Exception as e:
            logger.error("Failed to search vectors in Milvus: {}".format(e))
            sys.exit(1)
        # outid=[]
        # outdis=[]
        out={}
        out["id"]=[]
        out["distance"]=[]
        try:
            for result in res[0]:
                try:
                    if float(result.distance)<600:
                        sql="select image_path from " + collection_name + " where milvus_id = " + str(result.id) + " ;"
                        print(result.distance)
                        self.cursor.execute(sql)
                        rows=self.cursor.fetchall()
                        print(rows[0][0]) 
                        out["id"].append(rows[0][0])
                        score=0
                        score=100-((float(result.distance)/1200)*100)
                        out["distance"].append((score))
                        #out["id"]=rows[0][0]
                        #out["distance"]=score
                    else:
                        continue
                except:
                    continue
            # out["id"]=outid
            # outdis["distance"]=outdis
            return out
        except Exception as e:
            print("cann't search in table: ",e)
            
    def drop(self, collection_name, ids):
        # try:
        #     expr = "id in " + str(list(map(int, ids)))
        #     collection=Collection(self.collection_name)
        #     collection.load()
        #     res = collection.delete(expr)
        #     print(res)
        # except Exception as e:
        #     logger.error("Failed to delete data to Milvus: {}".format(e))
            
        #try:
        sql="select milvus_id from " + collection_name + " where image_path =  %s  ;"
        self.cursor.execute(sql,(ids,))
        rows=self.cursor.fetchall()
        print(rows[0][0])

        expr = "id in " + str(list(map(int, str(rows[0][0]))))
        connections.connect(host=self.host, port=self.port)
        collection=Collection(collection_name)
        collection.load()
        res = collection.delete(expr)
        print(res)

        sqldelete="delete from " + collection_name + " where image_path in ( %s  );"
        self.cursor.execute(sqldelete,(ids,))
       
        # except Exception as e:
        #     logger.error("Failed to delete data to Milvus: {}".format(e))


    def count(self,collection_name):
        try:
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            num = collection.num_entities
            logger.debug(
                "Successfully get the num:{} of the collection:{}".format(
                    num, collection_name
                )
            )
            return num
        except Exception as e:
            logger.error("Failed to count vectors in Milvus: {}".format(e))
            sys.exit(1)

    def show_postgres(self,collection_name):
        try:
            sql='select * from '+collection_name +";"
            self.cursor.execute(sql)
            rows=self.cursor.fetchall()
            print(rows)
            
            
        except Exception as e:
            logger.error("Fail to load postgres: {}".format(e))
if __name__=="__main__":
    con=Collection_Image()
    collection_name='Internal_image'
    collection="XO6_image"
    collection1=['H05','DHVB']

    ids= "asd"
    #con.drop(collection,ids)
    #con.show_postgres(collection)
    num = con.count(collection)
    num1=con.count(collection_name)
    print(num1)
    print(num)
