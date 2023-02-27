#!bin/bash
docker rm -f pg
docker pull postgres
docker run --name pg -e POSTGRES_PASSWORD=flexicommit -p 5432:5432 -d postgres -N 1000
docker exec -it pg psql -U postgres -c "create database flexi"
docker exec -it pg psql -U postgres -d flexi -c "SET default_transaction_isolation = 'serializable'"
docker exec -it pg psql -U postgres -d flexi -c "ALTER SYSTEM SET shared_buffers = '2GB'"
docker exec -it pg psql -U postgres -d flexi -c "DROP TABLE IF exists YCSB_MAIN"
docker exec -it pg psql -U postgres -d flexi -c "CREATE TABLE YCSB_MAIN (key VARCHAR(255) PRIMARY KEY,  value VARCHAR(255))"

docker rm -f mg
docker pull mongo
docker run --name mg -p 27019:27017 -d mongo
docker exec -it mg mongosh
use flexi
db.createUser({
    user: "tester",
    pwd: "123",
    roles: [{ role: "dbAdmin", db: "flexi" }]
});
