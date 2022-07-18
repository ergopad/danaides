# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
FROM python:3

COPY ./app /app
WORKDIR /app

# install system dependencies
RUN apt-get update \
  # && apt-get -y install netcat gcc postgresql \
  && apt-get -y install nano curl \
  && apt-get -y install python3-watchdog \
  && apt-get -y install openjdk-11-jdk \
  && apt-get clean

# install python dependencies
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

CMD tail /dev/null -f
