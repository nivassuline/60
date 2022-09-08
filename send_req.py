import threading
import requests
import time
from datetime import date

starttime = time.time()

while True:
    r = requests.get('http://127.0.0.1:5000/all')
    nowdate = date.today()
    nowtime = time.localtime()
    current_time = time.strftime("%H:%M:%S", nowtime)
    for i in r.json():
        try:
            j = requests.get(i[0])
            test = requests.get(f'http://127.0.0.1:5000/update_status?url={i[0]}&status_code={j.status_code}&last_updated={current_time}')
            print(test.json())
        except requests.exceptions.RequestException:
            test = requests.get(f'http://127.0.0.1:5000/update_status?url={i[0]}&status_code=404&last_updated={current_time}')
            print(test.json())
    time.sleep(60.0 - ((time.time() - starttime) % 60.0))

#http://127.0.0.1:5000/update_status?url=https://postman.com&status_code=500&last_updated=08/09/2022+02:56
# while True:
#     r = requests.get('http://127.0.0.1:5000/all')
#     print(r.text)
#     print(r.status_code)
#     time.sleep(60.0 - ((time.time() - starttime) % 60.0))