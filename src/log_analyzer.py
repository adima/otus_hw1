#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import re
import gzip
import argparse
import json
import logging
from copy import copy

from string import Template


with open('report.html', 'r') as f:
    try:
        webpage_template = f.read().decode('utf-8')
    except UnicodeDecodeError:
        webpage_template = f.read().decode('cp1251')


config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "ERRORS_THRSH": .8,
    "DONE_DIR": "./done"
}


def median(lst):
    """
    Медиана для списка значений
    :param lst: список значений
    :return:
    """
    sortedLst = sorted(lst)
    lstLen = len(lst)
    index = (lstLen - 1) // 2

    if lstLen % 2:
        return sortedLst[index]
    else:
        return (sortedLst[index] + sortedLst[index + 1])/2.0


def choose_log(log_dir):
    """
    Выбирает лог с последней датой
    :param log_dir: директория с логами
    :return:
    """
    fls = os.listdir(log_dir)
    fls = [fl for fl in fls if re.match(r'nginx-access-ui\.log-\d{6}(\.gz)?', fl)]
    if not fls:
        return
    else:
        return fls[-1]


def open_log_file(log_name):
    """
    Генератор, который возвращает строки файла лога
    :param log_name: имя лога
    :return:
    """
    reader = gzip if log_name[-2:] == 'gz' else io
    with reader.open(log_name) as f:
        for line in f:
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                line = line.decode('cp1251')
            yield line


def parse_log(log_dir, log_name, report_size, errors_thrshold, smoke_test=False):
    """
    Функция, которая извлекает заданные статистики из лога
    :param log_dir: директория лога
    :param log_name: имя файла лога
    :param report_size: количество url-ов с наибольшим временем обработки, которые должны быть в отчете
    :param errors_thrshold: допустимый процент строк, обработанных с ошибкой
    :param smoke_test: флаг для тестирования (обрабатывает первые 1000 строк если True)
    :return:
    """
    stats_url_time_sum = dict()
    line_ct = 0
    req_time_total = 0
    errors_ct = 0
    for n, line in enumerate(open_log_file(os.path.join(log_dir, log_name))):
        try:
            line_ct += 1
            l_spl = line.strip().split(' ')
            url = l_spl[7]
            req_time = float(l_spl[-1])
            req_time_total += req_time
            if n % 100 == 0:
                logging.info('Parsed %s lines' % n)
            if url in stats_url_time_sum.keys():
                stats_url_time_sum[url].append(req_time)
            else:
                stats_url_time_sum[url] = [req_time]

            if smoke_test and n > 1000:
                break
        except:
            logging.error("error parsing line %s " % n )
            errors_ct += 1

    if float(errors_ct) / line_ct >= errors_thrshold:
        logging.error("Error threshold exceeded")

    logging.info("parsed %s lines" % line_ct)
    result = ({'url': key,
               'count': len(value),
               'count_perc': len(value) / float(line_ct),
               'time_sum': sum(value),
               'time_perc': sum(value) / req_time_total,
               'time_avg': sum(value) / len(value),
               'time_max': max(value),
               'time_median': median(value),
               }
              for key, value in stats_url_time_sum.items())
    res_sorted = sorted(result, key=lambda x: x['time_sum'], reverse=True)

    return res_sorted[: report_size]


def get_config(gen_config, cfg_path):
    """
    Берет конфиг из заданного файла и апдейтит локальный конфиг
    :param cfg_path:  путь к конфигу
    :return:
    """
    with io.open(cfg_path) as cf:
        config_file = json.load(cf)

    upd_config = copy(gen_config)
    upd_config.update(config_file)
    return upd_config


def main(local_config, smoke_test=False):
    """
    Пайплайн обработки лога.
    :param local_config: Словарь конфига
    :param smoke_test: Файл отладки
    :return:
    """
    logging_path = local_config.get('logging_path')
    logging.basicConfig(filename=logging_path,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    if smoke_test:
        logging.info('Starting in smoke test mode')

    try:
        paths = [local_config['LOG_DIR'], local_config['REPORT_DIR'], local_config['DONE_DIR']]
        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)

        log_name = choose_log(local_config['LOG_DIR'])
        if log_name is None:
            logging.info("No logs found to parse")
            return
        log_date = log_name[-11:-3] if log_name.endswith('.gz') else log_name[-8:]

        result = parse_log(log_dir=local_config['LOG_DIR'],
                           log_name=log_name,
                           report_size=local_config['REPORT_SIZE'],
                           errors_thrshold=local_config['ERRORS_THRSH'],
                           smoke_test=smoke_test)

        rendered_temp = webpage_template.safe_substitute(dict(table_json=result))

        html_path = os.path.join(local_config['REPORT_DIR'], 'report-%s.%s.%s.html' % (log_date[:4],
                                                                                 log_date[4:6], log_date[6:8]))
        with io.open(html_path, 'w') as fh:
            fh.write(rendered_temp.decode('utf-8'))

        os.rename(os.path.join(local_config['LOG_DIR'], log_name),
                  os.path.join('done', log_name))
    except:
        logging.exception("Something unexpected happened")


def check_and_clear_test_folders(test_config):
    """
    Проверяет, созданы ли папки, и очищает созданные непустые для теста
    :param test_config: конфиг для теста
    :return:
    """
    paths = [test_config['LOG_DIR'], test_config['REPORT_DIR'], test_config['DONE_DIR']]
    for path in paths:
        if not os.path.exists(path):
            os.makedirs(path)
    for path in paths:
        fls = os.listdir(path)
        for fl in fls:
            os.remove(os.path.join(path, fl))


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description='Log analyzer arguments')
    arg_parser.add_argument("--config", type=str, default='./config')
    args = arg_parser.parse_args()
    config_loc = get_config(config, args.config)
    main(config_loc, smoke_test=False)
