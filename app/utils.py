import os
import sys
from bd import *
import yaml
import requests
from config import CONFIG
from datetime import timedelta

mydir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(mydir)


def print_stat(task_number):
    delta = timedelta(hours=-2)
    task_products = Products.select().where(Products.task_number == task_number)
    loaded_today = task_products.where(datetime.now() - delta - Products.time_check < timedelta(days=1)).count()
    loaded_all = task_products.where(Products.loaded == 1).count()
    bad = task_products.where(Products.bad == 1).count()
    all = task_products.count()
    loaded_in_5_minutes = task_products.select().where(
        (datetime.now() - delta - Products.time_check < timedelta(seconds=60 * 5))).count()
    print('loaded : {}, updated in last 24 hours : {}, bad : {}, all : {}, now speed: {} product/minute'.
          format(loaded_all, loaded_today, bad, all, round(loaded_in_5_minutes / 5, 2)))


def upload_tasks(values):
    batch = 50000
    load_task = values['-TASK_PATH-']
    task_name = load_task[load_task.rfind('/') + 1:]
    try:
        task_number = int(task_name[task_name.find('_v') + 2:-4])
    except:
        print("ERROR : can't find task version in name of the task."
              " Name must be your_name_v111.csv (111 - task number (int))")
        return None

    with open(load_task, 'r') as f:
        used = {}
        data_source = []
        for i, line in enumerate(f):
            if i < 2:
                continue
            else:
                id = int(line.split(',')[-1])
                if id in used.keys():
                    continue
                used[id] = 1
                data_source.append(
                    {'id': id, 'task_number': task_number, 'time_create': datetime.now(),
                     'time_check': datetime.fromisoformat('1900-01-01 00:00:00') + timedelta(
                         seconds=task_number)})

            if i % batch == 0:
                Products.insert_many(data_source).on_conflict(conflict_target=[Products.id],
                                                              update={
                                                                  'task_number': EXCLUDED.task_number,
                                                                  'time_create': EXCLUDED.time_create}).execute()
                print('loaded {} '.format(len(data_source)))
                used = {}
                data_source = []

        Products.insert_many(data_source).on_conflict(conflict_target=[Products.id],
                                                      update={
                                                          'task_number': EXCLUDED.task_number,
                                                          'time_create': EXCLUDED.time_create}).execute()
        print('loaded {} '.format(len(data_source)))

    print('task v{} has been uploaded'.format(task_number))


def reset_setting(values):
    with open('config.yaml', "r") as config_file:
        data_conf = yaml.safe_load(config_file)
    data_conf['parser']['k_threads'] = int(values['-THREAD_NUMBER-'])
    data_conf['parser']['processes'] = int(values['-PROCESS_NUMBER-'])
    data_conf['parser']['delay'] = int(values['-DELAY-'])
    data_conf['parser']['proxy_queue_size'] = 4 * data_conf['parser']['k_threads'] * data_conf['parser']['processes']
    data_conf['parser']['load_proxy_url'] = values['-PROXY_URL-']
    data_conf['parser']['alarm_id'] = values['-ALARMS-'].split(';')
    data_conf['parser']['alarm_push_time'] = int(values['-ALARM_TIME-'])
    with open('config.yaml', 'w') as outfile:
        yaml.dump(data_conf, outfile, default_flow_style=False)

    server = CONFIG['app']['server_with_parser']
    req = requests.post('http://{}/restart'.format(server), json=data_conf)
    print('restarting status {}'.format(str(req.json())))


def upload_proxies(values):
    f = open(values['-PROXIES_PATH-']).readlines()
    loaded_proxies = []
    for line in f:
        line = line.replace('\n', '')
        loaded_proxies.append(
            {'proxy': line, 'now_used': 0, 'last_used': datetime.fromisoformat('2000-01-01 00:00:00')})
    Proxies.delete().where(Proxies.now_used == 0).execute()
    Proxies.insert_many(loaded_proxies).on_conflict_ignore().execute()
    print(len(f), ' proxies has been added')
