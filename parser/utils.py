import pandas as pd
from collections import defaultdict
from yarl import URL
from queue import PriorityQueue
from aiohttp_socks import ProxyConnector
import aiohttp
import asyncio
from bd import *
from pydantic import BaseModel
from config import *
import requests


class InfoProduct(BaseModel):
    """
    Product description
    """
    id: int  # артикул товара
    name: str  # название товара
    price: float  # цена со скидкой
    old_price: float  # цена без скидки
    brand: str  # бренд товара
    saler: str  # продавец
    url: str  # ссылка на товар
    img_url: str  # ссылка на изображение товара
    rating: float  # рейтинг товара
    reviews_count: int  # кол-во отзывов
    size: str  # размер (касается одежды)
    product_count: int  # кол-во товара на складе
    delivery: bool  # есть ли доставка
    category: str  # категория товара


def get_value(data, name):
    for k, v in data.items():
        if name in k:
            return v
    return '{"items":[]}'


def get_info_from_json(id, data_json):
    """
    getting information about product from json
    """
    res = {}
    res['id'] = id
    res['url'] = 'https://www.ozon.ru/product/{}/'.format(res['id'])
    product = get_value(data_json['pdp']['productMobile'], 'productMobile')
    res['product_count'] = product['availability']['freeRest']
    res['name'] = product['title']
    res['price'] = product['price']['price']
    res['old_price'] = product['price']['originalPrice']
    res['brand'] = product['info']['brand']
    res['rating'] = product['feedback']['rating']
    res['reviews_count'] = product['feedback']['commentsCount']

    # Получаем информацию о доставке
    res['delivery'] = False
    try:
        track_key = get_value(data_json['pdp']['cartButton'], 'cartButton')['trackingInfo']['view']['key']
        tracking = json.loads(data_json['trackingPayloads'][track_key])
        res['delivery'] = bool(
            'availableDeliverySchema' in tracking.keys() and len(tracking['availableDeliverySchema']))
    except:
        try:
            tracking = get_value(data_json['pdp']['cartButton'], 'cartButton')['cellTrackingInfo']
            res['delivery'] = bool(
                'availableDeliverySchema' in tracking.keys() and len(tracking['availableDeliverySchema']))
        except:
            pass

    # Получаем ссылку на изображение
    try:
        res['img_url'] = product['images'][0]['url']
    except:
        res['img_url'] = 'None'

    res['saler'] = get_value(data_json['pdp']['crosslink'], 'crosslink')['title']
    res['category'] = product['info']['category']
    res['size'] = ''

    try:
        variants = get_value(data_json['pdp']['aspectsCompact'], 'aspectsCompact')['aspects']
        for param in variants:
            if param['title'] == 'Размер':
                for var in param['variants']:
                    if var['isSelected'] is True:
                        res['size'] = var['title']
    except:
        pass

    return res


async def get_product_info(id: int, token: str, proxy: str):
    """
    Loading product information from api, using proxy and auth token
    """
    headers = headers_for_getting_product.copy()
    headers['Authorization'] += token
    try:
        connector = ProxyConnector.from_url('socks5://{}'.format(proxy))
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.get('https://api.ozon.ru/composer-api.bx/page/json/v1?url=/products/{}/'.format(id),
                                   headers=headers) as req:
                data_json = await req.json()

    except Exception as ex:
        return {'id': id, 'error': str(type(ex))}

    try:
        res = get_info_from_json(id, data_json)
        return {'time_check': datetime.now(), 'data': InfoProduct.parse_obj(res).json(), 'bad': 0}
    except Exception as ex:
        if 'availability' in str(ex.args[0]):
            res = {}
            res['id'] = id
            res['url'] = 'https://www.ozon.ru/product/{}/'.format(res['id'])
            return {'time_check': datetime.now(), 'data': json.dumps(res), 'bad': 1}
        else:
            return {'id': id, 'error': str(ex.args[0])}


def alarm_message(message: str) -> None:
    """send message to alarm bot"""
    alarm = URL('https://alarmerbot.ru/')
    for key in CONFIG['parser']['alarm_id']:
        if not key is None:
            try:
                requests.post(alarm, {'key': key,
                                      'message': message})
            except Exception as ex:
                logging.info('alarm error for {}: {}'.format(key, str(type(ex))))


def update_task(task):
    """
    update queue of task from database

    :param (PriorityQueue) task: already exist taskQuery
    :return (PriorityQueue): updated taskQuery
    """
    for_update = []
    with db.atomic() as trans:
        active = Products.select().where(Products.paused == 0)
        products = active.where((Products.loaded == 0) & (Products.now_updating == 0)).order_by(Products.time_check) \
            .limit(max(0, CONFIG['parser']['task_size'] - task.qsize()))

        for product in products:
            for_update.append({'id': product.id})
            task.put((0, product.id))

        Products.insert_many(for_update).on_conflict(conflict_target=[Products.id],
                                                     update={'now_updating': 1}).execute()


async def get_auth_token(proxy):
    """
    Получаем токен аунтификации для прокси
    """
    logging.info('trying use {}'.format(proxy))
    try:
        # Делаем асинхронный запрос на Озон Апи для получения токена
        connector = ProxyConnector.from_url('socks5://{}'.format(proxy))
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post('https://api.ozon.ru/composer-api.bx/_action/initAuth',
                                    headers=headers_for_getting_token,
                                    data='{"clientId":"androidapp"}', timeout=30) as req:
                data_json = await req.json()
                if not 'authToken' in data_json.keys():
                    raise Exception('access_denied')

                token = data_json['authToken']['accessToken']
    except Exception as ex:
        try:
            error_message = str(ex.args[0])
        except:
            error_message = str(type(ex))

        # Разбор ошибок при получении токена
        logging.info('can not get token with {} Error: {}'.format(proxy, error_message))
        if error_message == 'access_denied':
            return {'type': 'access_denied', 'token': ''}
        elif 'Host unreachable' in error_message:
            return {'type': 'host_unreachable', 'token': ''}
        else:
            return {'type': 'connection_timeout', 'token': ''}
    return {'type': 'good', 'token': token}


async def update_proxy(need, cnt_requests):
    """
    loading `need` proxies with their auth token in database
    cnt_requests - count of request in one time
    """

    result = []
    while len(result) < need:
        # Берем из базы данных прокси, которые не использовались
        # хотя бы CONFIG['parser']['minimal_proxy_free_time'] секунд,
        # не лежат сейчас в очереди хороших проксе и не больше batch
        for_check = Proxies.select().where(
            datetime.now() - Proxies.last_used > timedelta(seconds=CONFIG['parser']['minimal_proxy_free_time'])) \
            .where(Proxies.now_used == 0).where(Proxies.now_active == 1).order_by(Proxies.last_used).limit(
            CONFIG['parser']['batch_load_proxies'])

        # Отсеиваем прокси, для которых есть активный токен авторизации
        updated = []
        need_update = []
        for proxy in for_check:
            if proxy.need_update_cookie == 0:
                updated.append({'proxy': proxy.proxy, 'now_used': 1, 'need_update_cookie': 0, 'token': proxy.token,
                                'last_used': datetime.fromisoformat('2000-01-01 00:00:00')})
                result.append((proxy.proxy, proxy.token))
                logging.info('loaded token from good')
            elif cnt_requests.value < CONFIG['parser']['proxy_threads']:
                need_update.append(proxy.proxy)
                logging.info('loaded new token')

        # Асинхронными запросами получаем токены для проксей
        tasks = []
        for proxy in need_update:
            tasks.append(asyncio.ensure_future(get_auth_token(proxy)))

        cnt_requests.value += len(tasks)
        checked = await asyncio.gather(*tasks)
        cnt_requests.value -= len(tasks)

        # Отсеиваем сломанные прокси
        for proxy, check in zip(need_update, checked):
            last_used = datetime.now()
            need_update_cookie = 1
            token = check['token']
            if check['type'] == 'host_unreachable':
                pass
            elif check['type'] == 'connection_timeout':
                last_used += timedelta(seconds=60 * 60)
            elif check['type'] == 'access_denied':
                last_used += timedelta(days=10)
            else:
                need_update_cookie = 0
                last_used = datetime.fromisoformat('2000-01-01 00:00:00')
                result.append((proxy, token))
            updated.append({'proxy': proxy, 'need_update_cookie': need_update_cookie,
                            'now_used': 1 - need_update_cookie, 'token': token, 'last_used': last_used})

        # Загружаем обновленные прокси в базу данных
        Proxies.insert_many(updated).on_conflict(conflict_target=[Proxies.proxy],
                                                 update={'need_update_cookie': EXCLUDED.need_update_cookie,
                                                         'now_used': EXCLUDED.now_used,
                                                         'token': EXCLUDED.token,
                                                         'last_used': EXCLUDED.last_used}).execute()
        if for_check.count() < CONFIG['parser']['batch_load_proxies']:
            break
        else:
            await asyncio.sleep(5)
    return result


def get_good_dataframe(products):
    """
    Creating a dataframe from `products`
    """
    all_products = []
    for product in products:
        if product.data == '':
            continue
        data = defaultdict(lambda: '-')
        for k, v in json.loads(product.data).items():
            data[k] = v
        res = {}
        res['datetime'] = datetime.date(datetime.now()).strftime("%d.%m.%Y")
        res['parsing_time'] = product.time_check.strftime("%H:%M:%S")
        res['platform'] = 'ozon.ru'
        res['name'] = data['name']
        if product.bad:
            res['price'] = res['old_price'] = '-'
        else:
            res['price'] = int(data['price'])
            res['old_price'] = int(data['old_price'])
        res['category'] = data['category'].replace('|', '/')
        res['brand'] = data['brand']
        res['seller'] = data['saler']
        res['inner_product_id'] = data['id']
        res['size'] = data['size']
        res['url'] = data['url']
        res['image'] = data['img_url']
        res['rating'] = data['rating']
        res['reviews_qty'] = data['reviews_count']
        res['quantity'] = data['product_count']
        res['region'] = '-'
        if data['delivery'] == True:
            res['warehouse_type'] = 'Доставка со склада продавца'
        else:
            res['warehouse_type'] = '-'
        if res['brand'] == 'None':
            res['brand'] = '-'
        if res['image'] == 'None':
            res['image'] = '-'

        all_products.append(res)
    df = pd.DataFrame(all_products)
    return df


def add_rus_name(file_name):
    """
    Add first line with russian description in file_name.csv
    """
    rus_name = ',Дата,Время,Площадка,Наименование,Цена,Старая цена,Категория,Бренд,Продавец,Код площадки,' \
               'Размер,Ссылка,Изображние,Рейтинг,Колиечество отзывов,Количество,Регион,Доставка\n'
    lines = [rus_name]
    with open(file_name, 'r') as f:
        lines += f.readlines()
    with open(file_name, 'w') as the_file:
        for item in lines:
            the_file.write(item)


def set_loaded_for_products(value) :
    """
    Set loaded = value for all products in database
    """
    batch = 1000000
    while not Products.get_or_none(Products.loaded != value) is None:
        ids = []
        now = Products.select().where(Products.loaded != value).limit(batch)
        for product in now:
            ids.append(product.id)
        Products.update(loaded=value).where(Products.id.in_(ids)).execute()


def download_tasks(reset_loaded=False):
    """
    downloading all tasks to server, and save it in loaded_tasks/
    if reset_loaded == True, then set 'loaded' = 0 in all products
    """
    date_str = datetime.now().date().isoformat()

    path = os.path.join('loaded_tasks/', date_str)
    if not os.path.exists(path):
        os.mkdir(path)

    # Проходимся по всем задачам
    task_uniq = Products.select(fn.DISTINCT(Products.task_number))
    for product in task_uniq:
        task_number = product.task_number
        all_task = Products.select().where(Products.task_number == task_number)
        all_task = all_task.where(Products.paused == 0).where(
            Products.time_check > datetime.fromisoformat('2000-01-01 00:00:00')).order_by(Products.id)

        batch = 50000
        big_batch = 1000000

        # Проходимся большими батчами (big_batch) по всем продуктам, подгружая в оперативку по big_batch товаров
        for i in range(all_task.count() // big_batch):

            task = all_task.limit(big_batch).offset(i * big_batch)
            now = []
            all = task.count()

            # Проходимся маленькими батчами (batch) и выгружаем данные в .csv
            for cnt, product in enumerate(task):
                now.append(product)
                if (cnt % batch == 0 or cnt == all - 1) and cnt != 0:
                    df = get_good_dataframe(now)
                    x = cnt + i * big_batch
                    df.to_csv(os.path.join(path, 'task_{}_{}.csv'.format(task_number, x)))
                    add_rus_name(os.path.join(path, 'task_{}_{}.csv'.format(task_number, x)))
                    logging.info('from v{} loaded {} products, all {}'.format(task_number, len(now), all))
                    now = []

            if task.count() != big_batch:
                break

    if reset_loaded:
        set_loaded_for_products(0)

    alarm_message('all tasks downloaded')
