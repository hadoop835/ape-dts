# setup test envrionments

## postgres

### source

```
docker run --name some-postgres-1 \
-p 5433:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-8 \
-d postgis/postgis:latest

docker exec -it some-postgres-1 bash
login: `psql -h 127.0.0.1 -U postgres -d postgres-p 5432 -W`
run `ALTER SYSTEM SET wal_level = logical;` and restart
```

### target

```
docker run --name some-postgres-2 \
-p 5434:5432 \
-e POSTGRES_PASSWORD=postgres \
-e TZ=Etc/GMT-7 \
-d postgis/postgis:latest
```

- create a test db for EUC_CN in both source and target

```
CREATE DATABASE postgres_euc_cn
  ENCODING 'EUC_CN'
  LC_COLLATE='C'
  LC_CTYPE='C'
  TEMPLATE template0;
```

## mysql

### source

```
docker run -d --name some-mysql-1 \
--platform linux/x86_64 \
-it \
-p 3307:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:5.7.40 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --default_time_zone=+08:00
```

### target

```
docker run -d --name some-mysql-2 \
--platform linux/x86_64 \
-it \
-p 3308:3306 -e MYSQL_ROOT_PASSWORD="123456" \
 mysql:8.0.31 --lower_case_table_names=1 --character-set-server=utf8 --collation-server=utf8_general_ci \
 --datadir=/var/lib/mysql \
 --user=mysql \
 --server_id=1 \
 --log_bin=/var/lib/mysql/mysql-bin.log \
 --max_binlog_size=100M \
 --gtid_mode=ON \
 --enforce_gtid_consistency=ON \
 --binlog_format=ROW \
 --binlog-transaction-compression \
 --binlog_rows_query_log_events=ON \
 --default_authentication_plugin=mysql_native_password \
 --default_time_zone="+07:00"
```

## mongo

### source

- 1. Create a Docker network:

```
docker network create mongo-network
```

- 2. Start 3 MongoDB containers for the replica set members:

```
docker run -d --name mongo1 --network mongo-network -p 9042:9042 mongo --replSet rs0  --port 9042
docker run -d --name mongo2 --network mongo-network -p 9142:9142 mongo --replSet rs0  --port 9142
docker run -d --name mongo3 --network mongo-network -p 9242:9242 mongo --replSet rs0  --port 9242
```

- 3. Connect to the primary (mongo1) and initialize the replica set:

```
docker exec -it mongo1 bash
mongosh --host localhost --port 9042
> config = {"_id" : "rs0", "members" : [{"_id" : 0,"host" : "mongo1:9042"},{"_id" : 1,"host" : "mongo2:9142"},{"_id" : 2,"host" : "mongo3:9242"}]}
> rs.initiate(config)
> rs.status()
```

- 4. Enable authentication on the admin database:

```
> use admin
> db.createUser({user: "ape_dts", pwd: "123456", roles: ["root"]})
```

- 5. Update /etc/hosts on Mac

```
127.0.0.1 mongo1 mongo2 mongo3
```

- 6. Test the connection from Mac

```
mongo "mongodb://ape_dts:123456@mongo1:9042/?replicaSet=rs0"
```

### target

```
docker run -d --name dst-mongo \
	-e MONGO_INITDB_ROOT_USERNAME=ape_dts \
	-e MONGO_INITDB_ROOT_PASSWORD=123456 \
  -p 27018:27017 \
	mongo
```

## redis
### images of redis versions
- redis:7.0
- redis:6.0
- redis:6.2
- redis:5.0
- redis:4.0
- redis:2.8

### source

```
docker run --name some-redis-1 \
-p 6380:6379 \
-d redis redis-server \
--requirepass 123456 \
--save 60 1 \
--loglevel warning
```

### target

```
docker run --name some-redis-2 \
-p 6381:6379 \
-d redis redis-server \
--requirepass 123456 \
--save 60 1 \
--loglevel warning
```