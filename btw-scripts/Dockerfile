# Dockerfile PostBOUND @ BTW'23

FROM ubuntu:focal

ARG user

ENV TZ=Europe/Berlin DEBIAN_FRONTEND=noninteractive

# run apt installs in separate steps to enable easier repetition if one install fails
# ubuntu's apt servers are having problems currently
RUN apt update
RUN apt install -y build-essential
RUN apt install -y bison flex
RUN apt install -y git
RUN apt install -y python3 python3-venv
RUN apt install -y r-base
RUN apt install -y texlive
RUN apt install -y curl wget vim

WORKDIR /postbound-btw23

RUN useradd -ms /bin/bash $user
RUN chown -R $user:$user /postbound-btw23
RUN chmod -R g+w,o+w /usr/local/lib/R/site-library
USER $user

RUN git clone --recurse-submodules --branch btw23-reproducibility --depth 1 https://github.com/rbergm/PostBOUND.git BTW23-PostBOUND
RUN cp BTW23-PostBOUND/btw-scripts/btw-* ./
