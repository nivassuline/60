import threading
import requests
import time
from datetime import date

starttime = time.time()


def get_url_list():
    url_list = []
    r = requests.get('http://10.0.0.10:80/db_data')  # send request to view all data in database
    for i in r.json():
        url_list.append(i[0])  # add the only the URL's to url_list
    return url_list


def send_req(url):
    nowtime = time.localtime()
    current_time = time.strftime("%H:%M:%S", nowtime)
    nowdate = date.today()
    # create all time instance's for last_update
    try:
        j = requests.get(url)  # get the status code for URL
        test = requests.get(
            f'http://10.0.0.10:80/update_status?url={url}&status_code={j.status_code}&last_updated={nowdate}+{current_time}')  # update status code and current time by URL
        print(test.json())
    except requests.exceptions.RequestException:
        # if URL returns exception update status code to 404 (not found)
        test = requests.get(
            f'http://10.0.0.10:80/update_status?url={url}&status_code=404&last_updated={nowdate}+{current_time}')
        print(test.json())


while True:
    threads = []
    for i in range(len(get_url_list())):  # loop over the number of threads
        t = threading.Thread(target=send_req, args=[get_url_list()[i]])  # pass URL's to send_req function
        t.start()
        threads.append(t)
    for thread in threads:
        thread.join()

    time.sleep(60.0 - ((time.time() - starttime) % 60.0))  # run threads every 60 minutes

