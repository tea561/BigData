docker cp $HOME/data/. namenode:/data

docker exec -it namenode hdfs dfs -test -d /data
if [ $? -eq 1 ]
then
  echo "[INFO]: Creating /data folder on HDFS"
  docker exec -it namenode hdfs dfs -mkdir /data
fi

docker exec -it namenode hdfs dfs -test -e /data/train.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding csv file in the /data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /data/train.csv /data/train.csv
fi


docker exec -it namenode hdfs dfs -test -e /data/porto.csv
if [ $? -eq 1 ]
then
  echo "[INFO]: Adding csv file in the /data folder on the HDFS"
  docker exec -it namenode hdfs dfs -put /data/porto.csv /data/porto.csv
fi
