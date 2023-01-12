# Dockerfile PostBOUND @ BTW'23

FROM ubuntu:focal

ARG user

ENV TZ=Europe/Berlin DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install -y build-essential bison flex git python3 python3-venv r-base texlive curl vim

WORKDIR /postbound-btw23

RUN useradd -ms /bin/bash $user
RUN chown -R $user:$user /postbound-btw23
RUN chmod -R g+w,o+w /usr/local/lib/R/site-library
USER $user

RUN git clone --recurse-submodules --branch btw23-reproducibility --depth 1 https://github.com/rbergm/PostBOUND.git BTW23-PostBOUND
RUN cp BTW23-PostBOUND/btw-scripts/btw-* ./
