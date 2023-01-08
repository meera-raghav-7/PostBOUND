#!/bin/bash

mkdir -p results
docker build -t btw-postbound --build-arg user=$USER . | tee docker-setup.log
docker run -dt \
    --name btw-postbound \
    --mount type=bind,source=$(pwd)/results,target=/postbound-btw23/BTW23-PostBOUND/results \
    btw-postbound
docker exec -t btw-postbound /postbound-btw23/btw-start.sh | tee experiments.log
