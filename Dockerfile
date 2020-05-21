FROM python:3.8-alpine

COPY requirements.txt /
RUN pip install -r requirements.txt

COPY opus_tmx_parser.py /

ENTRYPOINT ["python3", "./opus_tmx_parser.py"]
