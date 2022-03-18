FACTOR=$1
TAG_NAME=$2
YSQL_HOSTS="192.168.66.80 192.168.66.81 192.168.66.82"
YBIO_CONFIG=1
USER=yugabyte
#SCHEMA_NR_COUNTER=1
for HOST in $YSQL_HOSTS; do
  for NR in $(seq $FACTOR); do
    #SCHEMA=$(echo  -e "\x27$SCHEMA_NR_COUNTER\x27")
    CONFIG=$(echo  -e "\x27$YBIO_CONFIG\x27")
    TAG=$(echo -e "\x27$TAG_NAME\x27")
    ysqlsh -h $HOST -p 5433 -U $USER -v tag="$TAG" -v config="$CONFIG" -f runfile.sql >& run.output.$HOST.$NR.txt &
    #ysqlsh -h $HOST -p 5433 -U $USER -v tag="$TAG" -v schema="$SCHEMA" -v config="$CONFIG" -f runfile.sql >& run.output.$HOST.$NR.txt &
    #let SCHEMA_NR_COUNTER++
  done
done
wait
echo "Done"
