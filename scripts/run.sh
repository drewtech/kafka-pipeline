pwd
#!/bin/bash -eu

DIR=$(cd "${0%/*}" && pwd -P)
cd ${DIR}/..
docker-compose down
docker-compose build --no-cache 
docker-compose up -d
sleep 5s
aws s3 --endpoint-url http://localhost:4566 mb s3://drewtech.datapipeline.bucket
aws s3 --endpoint-url http://localhost:4566 cp ./files s3://drewtech.datapipeline.bucket --recursive
