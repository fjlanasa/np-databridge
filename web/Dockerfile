# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

WORKDIR /app

ADD ./utils ./utils
COPY ./web .

RUN pip3 install -r requirements.txt

ENV FLASK_APP=app

CMD [ "python3", "-m", "flask", "run", "--host", "0.0.0.0"]