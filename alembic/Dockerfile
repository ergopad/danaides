FROM python:3

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/app"

# install system dependencies
RUN apt-get update \
  # && apt-get -y install netcat gcc postgresql \
  && apt-get -y install nano curl \
  && apt-get clean

# install python dependencies
RUN python3 -m pip install --upgrade pip
RUN pip3 install -r requirements.txt

CMD tail /dev/null -f
