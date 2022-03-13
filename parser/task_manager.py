import logging
from threading import Thread
import traceback
import time
import utils
from utils import *
from queue import PriorityQueue
from bd import *
import asyncio
from config import *
import random
import aiohttp
import asyncio
from datetime import datetime, timedelta
import os
import multiprocessing
import peewee


async def async_thread(task, ready_task, good_proxy, used_proxy, cnt_requests):
    """
    Parsing products from Ozon in asynchronous threads
    """
    proxy, token = await good_proxy.get()
    while True:
        while task.qsize() == 0 or cnt_requests.value >= CONFIG['parser']['proxy_threads']:
            # logging.info('waiting')
            await asyncio.sleep(1)

        # Добавляем batch продуктов в очередь задач
        tasks = []
        now_task = CONFIG['parser']['batch_load_products']
        ids = []
        for i in range(now_task):
            priority, id = await task.get()
            ids.append((priority, id))
            tasks.append(asyncio.ensure_future(get_product_info(id, token, proxy)))
        logging.info('updating {} products'.format(now_task))

        # Парсим продукты
        cnt_requests.value += now_task
        ready = await asyncio.gather(*tasks)
        cnt_requests.value -= now_task

        # Добавляем скаченные продукты в очередь готов задач
        bad = 0
        for data, el in zip(ready, ids):
            if not 'error' in data.keys():
                ready_task.put((data['time_check'], el[1], data['data'], data['bad']))
            else:
                await task.put(el)
                logging.error('task_manager ' + data['error'] + ' ' + proxy + ' ' + str(el[1]))
                bad = 1

        # Обрабатываем случай, когда прокси забанили
        if bad:
            last_used = datetime.now() + timedelta(seconds=CONFIG['parser']['ban_when_cant_parse'])
            used_proxy.put({'last_used': last_used, 'need_update_cookie': 1, 'proxy': proxy, 'now_used': 0})
            proxy, token = await good_proxy.get()
        else:
            logging.info('{} products added to ready task'.format(len(ready)))
            await asyncio.sleep(CONFIG['parser']['delay'] + random.randint(0, 4))


class MainThread(Thread):
    """
    Manages tasks queue
    Manages proxies
    Sends statistics to alarm_bot
    """

    def __init__(self):
        Thread.__init__(self)
        self.task = get_priority_queue()
        self.ready_task = multiprocessing.Queue()
        self.good_proxy = multiprocessing.Queue()
        self.used_proxy = multiprocessing.Queue()
        self.cnt_requests = multiprocessing.Value('i', 0)

    def add_used_proxy_to_queue(self):
        """
        Move proxies from queue with used proxies to database
        """
        ready_proxies = []
        while self.used_proxy.qsize():
            ready_proxies.append(self.used_proxy.get())
        Proxies.insert_many(ready_proxies).on_conflict(conflict_target=[Proxies.proxy],
                                                       update={'now_used': 0,
                                                               'last_used': EXCLUDED.last_used,
                                                               'need_update_cookie': EXCLUDED.need_update_cookie}).execute()

    def add_loaded_product_to_db(self):
        """
        Move loaded products from queue with ready tasks to database
        """
        ready_products = []
        while self.ready_task.qsize():
            time_check, id, data, bad = self.ready_task.get()
            ready_products.append(
                {'time_check': time_check, 'id': id, 'data': data, 'bad': bad, 'loaded': 1, 'now_updating': 0})

        Products.insert_many(ready_products).on_conflict(conflict_target=[Products.id],
                                                         update={'time_check': EXCLUDED.time_check,
                                                                 'data': EXCLUDED.data,
                                                                 'bad': EXCLUDED.bad,
                                                                 'loaded': 1,
                                                                 'now_updating': 0}).execute()
        logging.info('loaded {} products'.format(len(ready_products)))

    def download_proxy_from_url(self, url):
        """
        Updating current activer proxy list by downloading proxies from url
        """
        try:
            loaded_proxies = []
            req = requests.get(url)
            f = req.text.split('\n')
            logging.info('proxy example : {}'.format(f[0]))
            used = {}
            for line in f:
                line = line.replace('\n', '')
                if not line in used.keys():
                    used[line] = 1
                    loaded_proxies.append({'proxy': line, 'now_used': 0, 'last_used': datetime.now()})
            Proxies.update(now_active=0).execute()
            Proxies.insert_many(loaded_proxies).on_conflict(conflict_target=[Proxies.proxy],
                                                            update={'now_active': 1}).execute()
            logging.info('loaded {} proxy'.format(len(loaded_proxies)))
        except :
            logging.info(traceback.format_exc())
            alarm_message('cant loading proxy from {}'.format(url))


    def send_alarm_stat(self, start):
        """
        Sending statistic about parser work to alarm_bot
        """
        speed = Products.select().where(Products.time_check > start).where(
            datetime.now() - Products.time_check < timedelta(
                seconds=CONFIG['parser']['count_speed_by_seconds'])).count()
        speed = speed * 60 / CONFIG['parser']['count_speed_by_seconds']

        alarm_message(
            'active {} proxies, bad proxies {}, proxies in work = {}, speed = {},'
            ' all = {}, bad = {}, loaded = {}'.
                format(Proxies.select().where(Proxies.now_active == 1).count(),
                       Proxies.select().where(Proxies.last_used > datetime.now()).where(
                           Proxies.now_active == 1).count(),
                       Proxies.select().where(Proxies.now_used == 1).count(),
                       round(speed, 1),
                       Products.select().count(),
                       Products.select().where(Products.bad == 1).count(),
                       Products.select().where(Products.loaded == 1).count()))

    def run(self):
        last = datetime.fromisoformat('2000-01-01 00:00:00')
        last_send_alarm = datetime.now()
        start = datetime.now()
        last_update_proxy = datetime.now() - timedelta(days=1)
        while True:
            if datetime.now() - last > timedelta(seconds=CONFIG['parser']['task_update_time']):
                update_task(self.task)
                updated = asyncio.run(
                    update_proxy(CONFIG['parser']['proxy_queue_size'] - self.good_proxy.qsize(), self.cnt_requests))
                for el in updated:
                    self.good_proxy.put(el)

                self.add_used_proxy_to_queue()
                self.add_loaded_product_to_db()
                logging.info('tasks updated, now its size is {}, proxy in queue {}'.format(self.task.qsize(),
                                                                                           self.good_proxy.qsize()))
                last = datetime.now()

            if datetime.now() - last_update_proxy > timedelta(seconds=60 * 3):
                last_update_proxy = datetime.now()
                if CONFIG['parser']['load_proxy_url'] != '':
                    self.download_proxy_from_url(CONFIG['parser']['load_proxy_url'])

                date_str = datetime.now().date().isoformat()
                path = os.path.join('loaded_tasks/', date_str)
                if not os.path.exists(path):
                    alarm_message('downloading all tasks')
                    download_thread = Thread(target=download_tasks, kwargs={'reset_loaded': True}, daemon=True)
                    download_thread.start()

            if datetime.now() - last_send_alarm > timedelta(seconds=CONFIG['parser']['alarm_push_time']):
                last_send_alarm = datetime.now()
                self.send_alarm_stat(start)
            time.sleep(1)
