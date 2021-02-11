FROM continuumio/miniconda3

SHELL ["/bin/bash", "-c"]

ADD .condarc /tmp/.condarc
ENV CONDARC=/tmp/.condarc

ADD environment.yml /tmp/environment.yml
WORKDIR /tmp
RUN [ "conda", "env", "create" ]

ADD ./app /app
WORKDIR /app

CMD source activate dev && python app.py
