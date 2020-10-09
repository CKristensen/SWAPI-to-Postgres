FROM python:3.8-alpine

WORKDIR /code

COPY requirements.txt requirements.txt
RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev 
RUN pip install psycopg2-binary
RUN pip install -r requirements.txt
RUN pip install update
COPY . .

CMD ["python", "main.py"]