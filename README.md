# mapreduce

This Repo implements [Mapreduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) in Python.

Master and Worker Communicates with each other via [RPyC](https://rpyc.readthedocs.io/en/latest/).


## Map Reduce Program
Users need to write their Map Reduce Program and put that in `mrapps` directory. The user program should implement two functions, `mapper` and `reducer` to perform map and reduce tasks respectively. `mrapps` directory contains a sample `word_count.py`  which counts frequency of words.

Once Map Reduce Program is ready and tested, then it needs to be serialized so that it can be distributed to workers.

`create_pickle.py` script can be used to serialize a map reduce program. Below command shows how to do it.

```
singhpradeepk:mapreduce$ python3 create_pickle.py --help
usage: Pickle Creator [-h] --mapr-app MAPR_APP [--pickle-file-path PICKLE_FILE_PATH]

Create the pickle file

optional arguments:
  -h, --help            show this help message and exit
  --mapr-app MAPR_APP   Name of the python file in directory mrapps without .py extension
  --pickle-file-path PICKLE_FILE_PATH
                        Path of the pickled map reduce job file
singhpradeepk:mapreduce $


python3 create_pickle.py --mapr-app word_count --pickle-file-path ./pickled_mrapps/
```

## How to run the program

Program can be run by using `run.sh` script. Before running value of variable `PICKLE_FILE` in `run.sh` should be changed to serialized map reduce program pickle file. Currently all master and workers are running on same machine inside different processes. 

```
bash run.sh
```

## Job Failure handling

* Heartbeat check - Master Keep checking heartbeat of workers periodically. If Any worker dies it 
reschedule the job to another worker.

* Restarting Job - If any worker dies after processing a mapper task, then Master will restart whole job again.