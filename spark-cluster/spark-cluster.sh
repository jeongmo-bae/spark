docker --version
docker-compose --version

cd ~ && mkdir cluster && cd cluster && mkdir spark-cluster && cd spark-cluster
vi docker-compose.yml

docker-compose up -d

docker ps
docker-compose ps

# spark master node 에 접근 및 spark-shell 실행
#docker exec -u 0 -it spark-master /bin/bash
docker exec -it spark-master /bin/bash
/opt/spark/bin/spark-shell --master spark://spark-master:7077
#/opt/spark/bin/spark-shell --master spark://spark-master:7077 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///opt/spark/logs

# spark-submit 을 통한 작업 제출 (scalar)
#docker exec -u 0 -it spark-master /bin/bash
docker exec -it spark-master /bin/bash
/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/spark_test.py 

docker logs spark-history
#docker-compose restart spark-history

docker-compose down
#docker-compose stop spark-master

