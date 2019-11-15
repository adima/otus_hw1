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
import unittest
import shutil


from string import Template





config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "ERRORS_THRSH": .8,
    "DONE_DIR": "./done"
}

def read_webpage_template(template_path='report.html'):
    """
    Функция читает и возвращает темплейт для отчета
    :param template_path:
    :return:
    """
    with open('report.html', 'r') as f:
        try:
            webpage_template = Template(f.read().decode('utf-8'))
        except UnicodeDecodeError:
            webpage_template = Template(f.read().decode('cp1251'))

    return webpage_template


def median(lst):
    """
    Медиана для списка значений
    :param lst: список значений
    :return: медианна
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
    :return: файл лога
    """
    fls = os.listdir(log_dir)
    log_name_re = re.compile(r'nginx-access-ui\.log-(?P<date>\d{4}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01]))(\.gz)?')

    log_filename, log_date = None, None
    for n, fl in enumerate(fls):
        file_name_re_match = log_name_re.match(fl)
        if file_name_re_match:
            if n == 0:
                log_filename = fl
                log_date = log_name_re.match(fl).group("date")
            else:
                if log_date < file_name_re_match.group("date"):
                    log_filename = fl
                    log_date = log_name_re.match(fl).group("date")

    return log_filename, log_date


def yield_line_from_log_file(log_name):
    """
    Генератор, который возвращает строки файла лога
    :param log_name: имя лога
    :return:
    """
    reader = gzip if log_name.endswith('gz') else io
    with reader.open(log_name) as f:
        for line in f:
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                line = line.decode('cp1251')
            yield line


def parse_line(line):
    """
    Функция, которая парсит строку лога
    :param line: строка лога
    :return: url, время запроса
    """
    l_spl = line.strip().split(' ')
    url = l_spl[7]
    req_time = float(l_spl[-1])
    return url, req_time


def make_log_stats(log_path, report_size, errors_thrshold, log_parse_func=yield_line_from_log_file, smoke_test=False):
    """
    Функция, которая извлекает заданные статистики из лога
    :param log_dir: директория лога
    :param log_name: имя файла лога
    :param report_size: количество url-ов с наибольшим временем обработки, которые должны быть в отчете
    :param errors_thrshold: допустимый процент строк, обработанных с ошибкой
    :param log_parse_func: функция для парсинга лога
    :param smoke_test: флаг для тестирования (обрабатывает первые 1000 строк если True)
    :return: отчет
    """
    stats_url_time_sum = dict()
    line_ct = 0
    req_time_total = 0
    errors_ct = 0
    for n, line in enumerate(log_parse_func(log_path)):
        try:
            line_ct += 1
            url, req_time = parse_line(line)
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
        raise ValueError("Error threshold exceeded")

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
    :return: обновленный конфиг
    """
    with io.open(cfg_path) as cf:
        config_file = json.load(cf)

    upd_config = copy(gen_config)
    upd_config.update(config_file)
    return upd_config


def save_report_to_file(log_stats, report_dir, log_date):
    """
    Записывает отчет в заданную директорию
    :param log_stats: Статистика для записи в отчет
    :param report_dir: Директория для отчета
    :param log_date: Дата лога, по которому делался отчет
    :return:
    """

    webpage_template = read_webpage_template()
    rendered_temp = webpage_template.safe_substitute(dict(table_json=log_stats))
    html_path = os.path.join(report_dir, 'report-%s.%s.%s.html' % (log_date[:4], log_date[4:6], log_date[6:8]))
    with io.open(html_path, 'w') as fh:
        fh.write(rendered_temp.decode('utf-8'))


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

        log_name, log_date = choose_log(local_config['LOG_DIR'])
        if log_name is None:
            logging.info("No logs found to parse")
            return

        log_stats = make_log_stats(log_path=os.path.join(local_config['LOG_DIR'], log_name),
                                   report_size=local_config['REPORT_SIZE'],
                                   errors_thrshold=local_config['ERRORS_THRSH'],
                                   smoke_test=smoke_test)

        save_report_to_file(log_stats, local_config['REPORT_DIR'], log_date)
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


class ResultTest(unittest.TestCase):
    def test_works_ok_if_no_logs(self):
        """
        Проверяет, работает ли пайплайн, если в папке нет файлов для обработки
        :return:
        """
        config = get_config('config_test')
        check_and_clear_test_folders(config)
        log_dir = config['LOG_DIR']
        res = choose_log(log_dir)
        self.assertTrue(res is None)

    def test_works_ok_with_gz(self):
        """
        Проверяет, отрабатывает ли пайплайн на файлах .gz
        :return:
        """
        config = get_config('config_test')
        check_and_clear_test_folders(config)
        log_dir = config['LOG_DIR']
        shutil.copy2('log_source/nginx-access-ui.log-20170630.gz', log_dir)
        main(config, smoke_test=True)
        res = os.listdir(config['REPORT_DIR'])
        self.assertTrue('report-2017.06.30.html' in res)



if __name__ == "__main__":
    unittest.main()
    arg_parser = argparse.ArgumentParser(description='Log analyzer arguments')
    arg_parser.add_argument("--config", type=str, default='./config')
    args = arg_parser.parse_args()
    config_loc = get_config(config, args.config)
    main(config_loc, smoke_test=False)
