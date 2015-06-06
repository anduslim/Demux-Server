#!/bin/bash

docker stop broker
docker stop db

docker rm broker
docker rm db

docker run --name=broker -dt -p 5432:5432 wheelaccess.info:8080/tinypostgresql
docker run --name=db -dt -p 61616:1884 wheelaccess.info:8080/tinymosquitto

if [ $? ] ; then
   echo "Running in loop"
   while true; do
   sleep 10000
   done
fi
