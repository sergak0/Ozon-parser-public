import sentry_sdk
from flask import Flask
import flask
import click
from task_manager import *
import sys

need_restart = 0
api = Flask(__name__)


@api.route('/download_all', methods=['GET'])
def api_download():
    """
    downloading all products, that have been ever loaded
    """
    download_tasks()
    return {'ok': 1}


@api.route('/set_paused', methods=['POST'])
def set_paused():
    """
    set paused parameter in products to data['paused']
    """
    task_number = flask.request.json['task_number']
    paused = flask.request.json['paused']
    now_task = Products.select().where(Products.task_number == task_number)
    cnt = now_task.where(Products.paused != paused).count()
    batch = 1000000
    while now_task.where(Products.paused != paused).count():
        ids = []
        now = now_task.where(Products.paused != paused).limit(batch)
        for product in now:
            ids.append(product.id)

        Products.update(paused=paused).where(Products.id.in_(ids)).execute()
        alarm_message('set paused = {}, for {} products'.format(paused, len(ids)))
    return {'ok': 1, 'cnt': cnt}


@api.route("/restart", methods=['POST'])
def api_restart() -> dict:
    """
    if it is settings overwrite, than just restart after a second,
    else use time.sleep(data['sleepeing']) and restart after it
    """
    global need_restart
    data = flask.request.json
    if not 'sleeping' in data.keys():
        with open('config.yaml', 'w') as outfile:
            yaml.dump(data, outfile, default_flow_style=False)
        need_restart = 1
    else:
        need_restart = data['sleeping']
    return {'ok': 1}


@api.route('/get', methods=['POST'])
def api_get_product() -> dict:
    """
    get product information by id
    """
    id = flask.request.json['id']
    q = Products.get_or_none(Products.id == id)
    if q is None or q.loaded == 0:
        return {"product whith id = {} doesn't loaded yet".format(id): 1}

    return json.loads(q.data)


async def queue_to_async(safe_queue, async_queue, type):
    """
    transfer Products/Proxies (in relation to type) from thread_safe_queue to async_queue in real time
    """
    while True:
        while safe_queue.qsize() > CONFIG['parser']['processes'] and async_queue.qsize() < CONFIG['parser'][type] // \
                CONFIG['parser']['processes']:
            el = safe_queue.get()
            await async_queue.put(el)
            await asyncio.sleep(0.1)
        await asyncio.sleep(2)


def process_worker(task, ready_task, good_proxy, used_proxy, cnt_requests):
    """
    run CONFIG['parser']['k_threads'] in process, also settings up queues transfer
    """
    good_proxy_async = asyncio.Queue()
    task_async = asyncio.Queue()
    for_run = []
    for i in range(CONFIG['parser']['k_threads']):
        for_run.append(
            asyncio.ensure_future(async_thread(task_async, ready_task, good_proxy_async, used_proxy, cnt_requests)))

    for_run.append(asyncio.ensure_future(queue_to_async(good_proxy, good_proxy_async, 'proxy_queue_size')))
    for_run.append(asyncio.ensure_future(queue_to_async(task, task_async, 'task_size')))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*for_run))


@click.command()
@click.option("--create_products", is_flag=True, help="Create (or recreate) database table of Products")
@click.option("--create_proxies", is_flag=True, help="Create (or recreate) database table of Proxies")
def main(create_products, create_proxies):
    alarm_message('running')
    if create_products:
        Products.drop_table()
        Products.create_table()

    if create_proxies:
        Proxies.drop_table()
        Proxies.create_table()

    if not os.path.exists('loaded_tasks/'):
        os.mkdir('loaded_tasks')

    api_thread = Thread(target=api.run, kwargs={'host': CONFIG['parser']['host'],
                                                'port': CONFIG['parser']['port']}, daemon=True)
    api_thread.start()

    # Говорим, что никакие продукты или товары сейчас не используются и не обновляются
    try:
        Products.update(now_updating=0).where(Products.now_updating == 1).execute()
        Proxies.update(now_used=0).where(Proxies.now_used == 1).execute()
    except peewee.OperationalError as ex:
        time.sleep(10)
        alarm_message(ex.args[0])
        os.system('kill {}'.format(os.getpid()))

    # Запускаем основной поток парсинга
    main_thr = MainThread()
    main_thr.daemon = True
    main_thr.start()

    # Меняем конфиги в зависимости от специальных значений
    if CONFIG['parser']['processes'] == -1:
        CONFIG['parser']['processes'] = os.cpu_count()

    logging.info('run in {} process'.format(CONFIG['parser']['processes']))

    # Запускаем процессы
    processes = [Process(target=process_worker, args=(
        main_thr.task, main_thr.ready_task, main_thr.good_proxy, main_thr.used_proxy, main_thr.cnt_requests,))
                 for i in range(CONFIG['parser']['processes'])]
    global need_restart
    for proces in processes:
        if need_restart or main_thr.is_alive() == False:
            break
        proces.start()

    # Мониторим состояние парсера и если необходимо перезапустить/какой-то поток умер
    # То убиваем парсер и перезапускаем его
    while True:
        if main_thr.is_alive() == False:
            need_restart = CONFIG['parser']['sleeping']

        if need_restart:
            for proces in processes:
                proces.terminate()
            alarm_message('finishing, need_restart = {}'.format(need_restart))
            time.sleep(need_restart)
            # sys.exit()
            os.system('kill {}'.format(os.getpid()))
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(filename=CONFIG['parser']['logging']['path'],
                        level=CONFIG['parser']['logging']['level'],
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    sentry_sdk.init(CONFIG['parser']['sentry_id'], traces_sample_rate=1.0)  # Отправка ошибок в личный кабинет sentry
    main()

