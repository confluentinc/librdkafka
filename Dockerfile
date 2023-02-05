FROM python:3.8-slim-buster

RUN apt-get update
RUN apt-get install -y tzdata ca-certificates git valgrind build-essential curl vim
RUN git clone https://github.com/confluentinc/librdkafka
RUN pip3 install trivup
RUN pip3 install jsoncomment

ENTRYPOINT ["/bin/sh"]
