FROM python:3

COPY pysnow/ /src/pysnow
WORKDIR /src/pysnow

RUN pip install -r requirements.txt

CMD [ "python", "./pysnow.py"]