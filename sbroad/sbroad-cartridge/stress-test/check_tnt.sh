#/bin/sh
#

RETRIES=50
TIMEOUT=10

[ "${TNT_HOST}" == "" ] && TNT_HOST=tarantool

echo "Wait before Tarantool cluster on ${TNT_HOST} is started"

RET=1
N=0
while [ $N -lt $RETRIES -a $RET -ne 0 ] ;do
  nc -z tarantool 3301 &>/dev/null
  RET=$?
  [ $RET -ne 0 ] && echo "Attempts left: $((RETRIES-N))" && sleep $TIMEOUT
  N=$((N+1))
done

if [ $RET -ne 0 ] ; then
  echo "Error: Tarantool cluster on ${TNT_HOST} is not started!"
  exit 1
else
  sleep 10
  echo "Tarantool cluster on ${TNT_HOST} is started"
fi

