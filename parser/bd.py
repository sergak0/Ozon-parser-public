from playhouse.pool import PostgresqlExtDatabase, PooledPostgresqlExtDatabase
from playhouse.postgres_ext import *
from datetime import datetime
from config import CONFIG


def get_db_Proxies_Products():
    db = PooledPostgresqlExtDatabase(CONFIG['parser']['bd']['bdname'],
                                     user=CONFIG['parser']['bd']['bduser'],
                                     password=CONFIG['parser']['bd']['bdpassword'],
                                     host=CONFIG['parser']['bd']['bdhost'],
                                     port=CONFIG['parser']['bd']['bdport'],
                                     max_connections=1000,
                                     keepalives=1,
                                     autorollback=True
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
        last_used = DateTimeField(default=datetime.now)
        now_active = IntegerField(default=1)
        token = TextField(default='')
        need_update_cookie = IntegerField(default=1)

        class Meta:
            database = db

    return db, Proxies, Products


db, Proxies, Products = get_db_Proxies_Products()

if __name__ == '__main__':
    print("#" + CONFIG['parser']['load_proxy_url'] + "#")
