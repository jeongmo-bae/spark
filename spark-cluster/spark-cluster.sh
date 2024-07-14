docker --version
docker-compose --version

cd ~ && mkdir cluster && cd cluster && mkdir spark-cluster && cd spark-cluster
vi docker-compose.yml

docker-compose up -d

docker ps
docker-compose ps

# spark master node 에 접근 및 spark-shell 실행
docker exec -it spark-master /bin/bash
/opt/spark/bin/spark-shell --master spark://spark-master:7077
#/opt/spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/spark/logs

docker logs spark-history
#docker-compose restart spark-history

docker-compose down
#docker-compose stop spark-master

