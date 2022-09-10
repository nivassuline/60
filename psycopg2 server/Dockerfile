# syntax=docker/dockerfile:1

FROM python:3.10

WORKDIR /app

COPY app.py .

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

ENTRYPOINT [ "python3" ]

CMD ["app.py"]