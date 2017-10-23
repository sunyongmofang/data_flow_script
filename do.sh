#!/bin/bash

/home/liunannan/spark/bin/spark-submit --master spark://jesgoo05:7077 --total-executor-cores $1 --executor-memory $2 $3 $4 $5 $6 $7
