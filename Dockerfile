FROM python:3.10-slim

WORKDIR /work

COPY ./requirements.txt /work/
RUN pip install -r requirements.txt

COPY ./ris_3 /work/ris_3