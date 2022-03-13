import os
import sys
import traceback
import sentry_sdk
import PySimpleGUI as sg
from bd import *
from datetime import timedelta
from utils import *
import time
import yaml
import requests
from config import CONFIG

mydir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(mydir)


def app_run(window):
    while True:
        event, values = window.read()
        # print(event,values)
        if event in (None, 'Exit', 'Cancel', sg.WIN_CLOSED):
            break

        # db.connect()
        if event == "upload task":
            upload_tasks(values)

        elif event == "reset settings":
            reset_setting(values)

        elif event == 'get statistics by task':
            if values['-TASK_NUMBER-'].isdigit():
                task_number = int(values['-TASK_NUMBER-'])
                print_stat(task_number)
            else:
                print('ERROR : task number for statistics must be integer')

        elif event == 'get proxies statistics':
            proxies_in_work = Proxies.select().where(Proxies.now_used >= 1).count()
            all_proxies = Proxies.select().count()
            bad_proxies = Proxies.select().where(Proxies.last_used > datetime.now()).count()
            print('proxies in work : {}, bad proxies (get access denied) : {}, all proxies : {}'.format(proxies_in_work,
                                                                                                        bad_proxies,
                                                                                                        all_proxies))
        elif event == 'get product by id':
            req = requests.post('http://{}/get'.format(CONFIG['app']['server_with_parser']),
                                json={'id': int(values['-ID-'])})
            data = req.json()
            for key, val in data.items():
                print(key, ' : ', val)

        elif event == 'upload proxies':
            upload_proxies(values)

        elif event == 'full statistics':
            task_uniq = Products.select(fn.DISTINCT(Products.task_number))
            for product in task_uniq:
                if datetime.now() - Products.get(Products.task_number == product.task_number).time_create < timedelta(
                        hours=int(values['-HOURS-'])):
                    print('task_number = {}'.format(product.task_number), end=' ')
                    print_stat(product.task_number)

        elif event == 'download all':
            server = CONFIG['app']['server_with_parser']
            requests.get('http://{}/download_all'.format(server))
            print('{} products have been downloaded'.format(Products.select().where(Products.loaded == 1).count()))

        elif event == 'pause task' or event == 'resume task':
            if values['-TASK_NUMBER-'].isdigit():
                task_number = int(values['-TASK_NUMBER-'])
                if event == 'pause task':
                    data = requests.post('http://{}/set_paused'.format(CONFIG['app']['server_with_parser']),
                                         json={'task_number': task_number, 'paused': 1}).json()
                    print('paused {} products'.format(data['cnt']))
                else:
                    data = requests.post('http://{}/set_paused'.format(CONFIG['app']['server_with_parser']),
                                         json={'task_number': task_number, 'paused': 0}).json()
                    print('resumed {} products'.format(data['cnt']))
                requests.post('http://{}/restart'.format(CONFIG['app']['server_with_parser']), json={'sleeping': 1})
            else:
                print('ERROR : task number for statistics must be integer')

        time.sleep(0.01)
        # db.close()


class App:
    buttons = [[sg.Button('upload task', size=(15, 1)), sg.Button('full statistics', size=(15, 1)),
                sg.Button('download all', size=(15, 1))],
               [sg.Button('upload proxies', size=(15, 1)), sg.Button('get proxies statistics', size=(22, 1)),
                sg.Button('reset settings', size=(15, 1))],
               [sg.Button('pause task', size=(15, 1)), sg.Button('resume task', size=(15, 1))]]
    left_column = 25
    layout = [
        [sg.Text('path to task', size=(left_column, 1)),
         sg.InputText(key='-TASK_PATH-', default_text='tasks/task_v1.csv'), sg.FileBrowse()],
        [sg.Text('path to proxies', size=(left_column, 1)),
         sg.InputText(key='-PROXIES_PATH-', default_text='proxies.txt'), sg.FileBrowse()],
        [sg.Text('number of processes', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-PROCESS_NUMBER-", default_text=str(CONFIG['parser']['processes']))],
        [sg.Text('number of threads', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-THREAD_NUMBER-", default_text=str(CONFIG['parser']['k_threads']))],
        [sg.Text('delay between download tasks', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-DELAY-", default_text=str(CONFIG['parser']['delay']))],
        [sg.Text('proxy download from', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-PROXY_URL-", default_text=CONFIG['parser']['load_proxy_url'])],
        [sg.Text('alarm ids', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-ALARMS-", default_text=';'.join(CONFIG['parser']['alarm_id']))],
        [sg.Text('alarm push time', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-ALARM_TIME-", default_text=CONFIG['parser']['alarm_push_time'])],
        [sg.Text('show statistic by last hours', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-HOURS-", default_text='100')],
        [sg.Text('task number', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-TASK_NUMBER-", default_text='1'),
         sg.Button('get statistics by task', size=(25, 1))],
        [sg.Text('product id for "get product by id"', size=(left_column, 1)),
         sg.InputText(size=(25, 1), key="-ID-", default_text='172191602'),
         sg.Button('get product by id', size=(25, 1))],
        [sg.Column(buttons, element_justification='center', pad=(50, 0))],
        [sg.Output(size=(65, 15))],
    ]

    def __init__(self):
        sg.set_options(font=("Any", 16), button_color=('blue', None))
        self.window = sg.Window('OZON parser', self.layout)

    def run(self):
        while True:
            try:
                app_run(self.window)
                break
            except KeyboardInterrupt:
                break
            except:
                print('что-то пошло не так, попробуйте еще раз')
                print(traceback.format_exc())
                logging.info(traceback.format_exc())


if __name__ == '__main__':
    sentry_sdk.init(CONFIG['parser']['sentry_id'], traces_sample_rate=1.0)
    logging.basicConfig(filename='sample.log',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    app = App()
    app.run()
