from playhouse.pool import PostgresqlExtDatabase, PooledPostgresqlExtDatabase
from playhouse.postgres_ext import *
from datetime import datetime
from config import CONFIG
import requests

db = PooledPostgresqlExtDatabase(CONFIG['app']['bd']['bdname'],
                                 user=CONFIG['app']['bd']['bduser'],
                                 password=CONFIG['app']['bd']['bdpassword'],
                                 host=CONFIG['app']['bd']['bdhost'],
                                 port=CONFIG['app']['bd']['bdport'],
                                 max_connections=1000,
                                 keepalives=1,
                                 autorollback=True
                                 # keepalives_idle=30,
                                 # keepalives_interval=10,
                                 # keepalives_count=5
                                 )


class Products(Model):
    """
    База данных
    """
    id = IntegerField(unique=True)
    time_check = DateTimeField(default=datetime.now)
    loaded = IntegerField(default=0)
    paused = IntegerField(default=0)
    bad = IntegerField(default=0)
    now_updating = IntegerField(default=0)
    task_number = IntegerField(default=0)
    time_create = DateTimeField(default=datetime.now)
    data = JSONField(default="")

    class Meta:
        database = db


class Proxies(Model):
    proxy = TextField(unique=True)
    now_used = IntegerField(default=0)
    now_active = IntegerField(default=1)
    last_used = DateTimeField(default=datetime.now)
    token = TextField(default='')
    need_update_cookie = IntegerField(default=1)

    class Meta:
        database = db


if __name__ == '__main__':
    print(Products.select().where(Products.loaded == 0).count(), Products.select().count())
    requests.post('http://{}/restart'.format(CONFIG['app']['server_with_parser']), json={'sleeping': 1800})
