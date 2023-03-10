# python -m mrapps.word_count --input-file /Users/singhpradeepk/workspace/code/mit-6.824-labs/src/main/pg-being_ernest.txt --output-file mr-s-wc-all
# sort mr-s-wc-all > correct-mr-s-wc-all
# rm mr-s-wc-all

DIR="tmp"
mkdir $DIR

SECONDS=0
python -m mapr1.controller.master --input-files ./books/\*.txt &

sleep 5

duration1=$SECONDS
python -m mapr1.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12348 &
python -m mapr1.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12347 &
python -m mapr1.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12346 &
python -m mapr1.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12349 &
python -m mapr1.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12350 &

wait
duration2=$SECONDS
sort ./$DIR/mr-out* | grep . > mapr-mr1-p-wc-all
rm  ./$DIR/mr-*


python -m mapr2.controller.master --input-files ./books/\*.txt &

sleep 5

duration3=$SECONDS

python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12348 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12347 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12346 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12349 &
python -m mapr2.controller.worker --master-host '127.0.0.1' --master-port 12345 --tmp-dir ./$DIR --pickle-file-path pickled_mrapps/word_count.pb --worker-port 12350 &

wait
duration4=$SECONDS
sort ./$DIR/mr-out* | grep . > mapr-mr2-p-wc-all

rm  ./$DIR/mr-*


if cmp  mapr-mr2-p-wc-all mapr-mr1-p-wc-all
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same
  echo '---' wc test: FAIL
fi
echo "mapr1 execution time in seconds" $((duration2 - duration1))
echo "mapr2 execution time in seconds" $((duration4 - duration3))

rm -rf $DIR