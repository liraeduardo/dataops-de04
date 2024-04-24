FROM python:3.10

RUN mkdir -p /usr/src/python-app

WORKDIR /usr/src/python-app

COPY . /usr/src/python-app/

RUN pip install --upgrade pip

RUN pip install -r requirements.txt
