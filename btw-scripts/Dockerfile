# Dockerfile PostBOUND @ BTW'23

FROM ubuntu:focal

ARG user

WORKDIR /postbound-btw23
COPY btw-* ./

ENV TZ=Europe/Berlin DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential bison flex git python3 python3-venv r-base texlive curl vim
RUN useradd -s /bin/bash $user
USER $user
