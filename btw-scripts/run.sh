#!/bin/bash

DOCKER_ROOT=/postbound-btw23
USER=$(whoami)

docker build -t btw-postbound --build-arg user=$USER . | tee docker-setup.log
docker run -dt \
    --name btw-postbound \
    --shm-size 2G \
    --user $USER \
    btw-postbound


docker exec -t --user $USER btw-postbound $DOCKER_ROOT/btw-start.sh | tee experiments.log
docker container cp btw-postbound btw-postbound:$DOCKER_ROOT/BTW23-PostBOUND/results .
