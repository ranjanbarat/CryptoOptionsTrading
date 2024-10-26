FROM python:3.13-slim as base

LABEL authors="ranjanbarat"

WORKDIR /crypto-app-dev

COPY . .

RUN pip3 install -r requirements.txt

CMD ["python3", "./src/main.py" ]
