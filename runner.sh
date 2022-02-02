FACTOR=$1
TAG=$(echo -e "\x27$2\x27")
YSQL_HOSTS="10.9.112.14 10.9.207.252 10.9.140.59"
USER=yugabyte
for HOST in $YSQL_HOSTS; do
  for NR in $(seq $FACTOR); do
    ysqlsh -h $HOST -p 5433 -U $USER -v tag="$TAG" -f runfile.sql >& run.output.$HOST.$NR.txt &
  done
done
wait
echo "Done"
