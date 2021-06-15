# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

ADD ./utils ./utils
COPY ./job .

RUN pip3 install -r requirements.txt

CMD [ "echo", "$HOME"]