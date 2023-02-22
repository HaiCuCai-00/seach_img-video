## Usage
```
pip install -r requirements.txt

```

Start database server:
```bash 
docker-compose up -d
##docker run -p 3306:3306 -e MYSQL_ROOT_PASSWORD=123456 -d mysql:5.7
docker run --name postgres0 -d  -p 5438:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres
docker logs postgres0 --tail 6
```

