#!/bin/bash

DOCKER_ROOT=/postbound-btw23
USER=$(whoami)

mkdir -p results
docker build -t btw-postbound --build-arg user=$USER . | tee docker-setup.log
docker run -dt \
    --name btw-postbound \
    --mount type=bind,source=$(pwd)/results,target=$DOCKER_ROOT/BTW23-PostBOUND/results \
    --user $USER \
    btw-postbound

if [ -d "../imdb_data" ] ; then
    docker container cp ../imdb_data btw-postbound:$DOCKER_ROOT
fi

docker exec -t --user $USER btw-postbound $DOCKER_ROOT/btw-start.sh | tee experiments.log
