from pymilvus import connections, DataType, FieldSchema, CollectionSchema, Collection, utility
from pymilvus.orm import schema
from loguru import logger
import sys
import psycopg2

class Collection_Video():
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
            
        except Exception as e:
            logger.error("Failed to load data to Milvus: {}".format(e))
        
        try:
            sql = "CREATE TABLE if not exists " + collection_name + " (ids bigint, title text, time int);"
            self.cursor.execute(sql)
            self.conn.commit()
            print("create postgres table successfully!")
        except Exception as e:
            print("cann't create a postgres table: ",e)

    def insert(self,collection_name,idvideo, vectors):
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
             
        except Exception as e:
            logger.error("Failed to load data to Milvus: {}".format(e))
            
        try:
            count = 0
            for id in range(len(ids)):
                self.cursor.execute('INSERT INTO '+ collection_name +' (ids, title, time) values (%s,%s,%s)' ,(ids[id],idvideo,count))
                print("complete insert done")
                self.conn.commit() 
                count+=1 
            logger.debug("insert successfully database")
        except Exception as e:
            print("cann't create a postgres table: ",e)

    def create_index(self, collection_name):
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
    def delete_collection(self,collection_name ):
        try:
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            collection.drop()
            logger.debug("Successfully drop collection!")
            return "ok"
        except Exception as e:
            logger.error("Failed to drop collection: {}".format(e))
            sys.exit(1)
    
    def search_vectors(self, collection_name,vectors, top_k):
        try:
            out={}
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            search_params={"metric_type": "L2", "params": {"nprobe": 10}}
            res=collection.search(
                vectors,
                anns_field="embedding",
                param=search_params,
                limit=top_k,
            )
            logger.debug("Successfully search in collection: {}".format(res))
            
        except Exception as e:
            logger.error("Failed to search vectors in Milvus: {}".format(e))
        
        try:
            time_list=[]
            for result in res[0]:
                sql = "select time, title from " + collection_name + " where ids = " + str(result.id) + ";"
                self.cursor.execute(sql)
                rows=self.cursor.fetchall()
                print(rows[0][1])

                for row in rows:
                    print(row[0])
                    #print('0',row[0])
                    
                    time=row[0]
                    hour=time//3600
                    time=time%3600
                    minute=time//60
                    time=time%60
                    time_line=f"{hour}:{minute}:{time}"
                    
                    time_list.append(time_line)
                    print(f"time: {hour}:{minute}:{time}")
                    out["time"]=time_line
            out["id"]=rows[0][1]
            return out
        except Exception as e:
            print("cann't search in table: ",e)

    def drop(self, collection_name, ids):
        sql="select ids from " + collection_name + " where title =  %s  ;"
        self.cursor.execute(sql,(ids,))
        rows=self.cursor.fetchall()
        # print(rows)
        for row in rows:
            expr = "id in " + str(list(map(int, str(row[0]))))
            connections.connect(host=self.host, port=self.port)
            collection=Collection(collection_name)
            collection.load()
            res = collection.delete(expr)
            print(res)
        sqldelete="delete from " + collection_name + " where title in ( %s  );"
        self.cursor.execute(sqldelete,(ids,))

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
            sql='select * from '+collection_name +" ;"
            self.cursor.execute(sql)
            rows=self.cursor.fetchall()
            print(rows)
            
        except Exception as e:
            logger.error("Fail to load postgres: {}".format(e))

    
if __name__=="__main__":
    milvus=Collection_Video()
   
    collection_name="dhvb_video"
    ids='abcd123'
    #milvus.drop(collection_name, ids)
    milvus.show_postgres(collection_name)