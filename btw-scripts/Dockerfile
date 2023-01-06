# Dockerfile PostBOUND @ BTW'23

FROM ubuntu:focal

WORKDIR /postbound-btw23
COPY btw-* ./

ENV TZ=Europe/Berlin DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential bison git python3 python3-venv r-base texlive curl vim

# Should the start script actually be part of the Dockerfile, or rather be called explicitly after Dockerfile was used?
#CMD ./btw-start.sh
