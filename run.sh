# python -m mrapps.word_count --input-file /Users/singhpradeepk/workspace/code/mit-6.824-labs/src/main/pg-being_ernest.txt --output-file mr-s-wc-all
# sort mr-s-wc-all > correct-mr-s-wc-all
# rm mr-s-wc-all

DIR="tmp"
mkdir $DIR

PICKLE_FILE="word_count.pb"
SECONDS=0
python -m mapr2.controller.master --port 12345 --input-files ./books/\*.txt &

sleep 5


duration1=$SECONDS

python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/$PICKLE_FILE --worker-port 12348 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/$PICKLE_FILE --worker-port 12347 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/$PICKLE_FILE --worker-port 12346 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/$PICKLE_FILE --worker-port 12349 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/$PICKLE_FILE --worker-port 12350 &

wait
duration2=$SECONDS
sort ./$DIR/mr-out* | grep . > mapr-mr2-p-wc-all

rm  ./$DIR/mr-*


echo "mapr2 execution time in seconds" $((duration2 - duration1))

rm -rf $DIR