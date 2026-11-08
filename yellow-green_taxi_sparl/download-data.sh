# this code immeditly stop our bash code if something wrong happens like errors 
set -e
# and $$1 and $2 means they are configurable which means we can enter these valeus in bash while running as argument
TAXI_TYPE=$1 #"yellow" 
YEAR=$2 #2020

URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data"
#we are lopping here looks like python 
for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}` #02d means, there must be 2 len int num here, for example if num is 1-9 it puts like 01 02 03 etc ..
    
    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${MONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"
    # if local path is not exist this cmnd creates it 
    mkdir -p ${LOCAL_PREFIX}
    # it downloads and save to local path 
    wget ${URL} -O ${LOCAL_PATH}
done