# syntax=docker/dockerfile:1

FROM python:3.10

WORKDIR /app

COPY send_req.py .

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

EXPOSE 80

ENTRYPOINT [ "python3" ]

CMD ["send_req.py"]