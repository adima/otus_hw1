#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import re
import gzip
import argparse
import json
import logging
import unittest
from copy import copy
import shutil

from string import Template


webpage_template = Template("""<!doctype html>

<html lang="en">
<head>
  <meta charset="utf-8">
  <title>rbui log analysis report</title>
  <meta name="description" content="rbui log analysis report">
  <style type="text/css">
    html, body {
      background-color: black;
    }
    th {
      text-align: center;
      color: silver;
      font-style: bold;
      padding: 5px;
      cursor: pointer;
    }
    table {
      width: auto;
      border-collapse: collapse;
      margin: 1%;
      color: silver;
    }
    td {
      text-align: right;
      font-size: 1.1em;
      padding: 5px;
    }
    .report-table-body-cell-url {
      text-align: left;
      width: 20%;
    }
    .clipped {
      white-space: nowrap;
      text-overflow: ellipsis;
      overflow:hidden !important;
      max-width: 700px;
      word-wrap: break-word;
      display:inline-block;
    }
    .url {
      cursor: pointer;
      color: #729FCF;
    }
    .alert {
      color: red;
    }
  </style>
</head>

<body>
  <table border="1" class="report-table">
  <thead>
    <tr class="report-table-header-row">
    </tr>
  </thead>
  <tbody class="report-table-body">
  </tbody>

  <script type="text/javascript" src="https://ajax.googleapis.com/ajax/libs/jquery/3.2.1/jquery.min.js"></script>
  <script type="text/javascript" src="jquery.tablesorter.min.js"></script> 
  <script type="text/javascript">
  !function($) {
    var table = $table_json;
    var reportDates;
    var columns = new Array();
    var lastRow = 150;
    var $table = $(".report-table-body");
    var $header = $(".report-table-header-row");
    var $selector = $(".report-date-selector");

    $(document).ready(function() {
      $(window).bind("scroll", bindScroll);
        var row = table[0];
        for (k in row) {
          columns.push(k);
        }
        columns = columns.sort();
        columns = columns.slice(columns.length -1, columns.length).concat(columns.slice(0, columns.length -1));
        drawColumns();
        drawRows(table.slice(0, lastRow));
        $(".report-table").tablesorter(); 
    });

    function drawColumns() {
      for (var i = 0; i < columns.length; i++) {
        var $th = $("<th></th>").text(columns[i])
                                .addClass("report-table-header-cell")
        $header.append($th);
      }
    }

    function drawRows(rows) {
      for (var i = 0; i < rows.length; i++) {
        var row = rows[i];
        var $row = $("<tr></tr>").addClass("report-table-body-row");
        for (var j = 0; j < columns.length; j++) {
          var columnName = columns[j];
          var $cell = $("<td></td>").addClass("report-table-body-cell");
          if (columnName == "url") {
            var url = "https://rb.mail.ru" + row[columnName];
            var $link = $("<a></a>").attr("href", url)
                                    .attr("title", url)
                                    .attr("target", "_blank")
                                    .addClass("clipped")
                                    .addClass("url")
                                    .text(row[columnName]);
            $cell.addClass("report-table-body-cell-url");
            $cell.append($link);
          }
          else {
            $cell.text(row[columnName]);
            if (columnName == "time_avg" && row[columnName] > 0.9) {
              $cell.addClass("alert");
            }
          }
          $row.append($cell);
        }
        $table.append($row);
      }
      $(".report-table").trigger("update"); 
    }

    function bindScroll() {
      if($(window).scrollTop() == $(document).height() - $(window).height()) {
        if (lastRow < 1000) {
          drawRows(table.slice(lastRow, lastRow + 50));
          lastRow += 50;
        }
      }
    }

  }(window.jQuery)
  </script>
</body>
</html>""")

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
    fls = [f for f in fls if re.match(r'nginx-access-ui\.log-\d{6}(\.gz)?', f)]
    if len(fls) == 0:
        return
    else:
        return sorted(fls)[-1]


def open_log_file(log_name):
    """
    Генератор, который возвращает строки файла лога
    :param log_name: имя лога
    :return:
    """
    reader = gzip if log_name[-2:] == 'gz' else io
    with reader.open(log_name) as f:
        for l in f:
            try:
                l = l.decode('utf-8')
            except:
                l = l.decode('cp1251')
            yield l


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
    for n, l in enumerate(open_log_file(os.path.join(log_dir, log_name))):
        try:
            line_ct += 1
            l_spl = l.strip().split(' ')
            url = l_spl[7]
            req_time = float(l_spl[-1])
            req_time_total += req_time
            if n % 100 == 0:
                print('Parsed %s lines' % n)
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


def get_config(cfg_path):
    """
    Берет конфиг из заданного файла и апдейтит локальный конфиг
    :param cfg_path:  путь к конфигу
    :return:
    """
    global config
    with io.open(cfg_path) as cf:
        config_file = json.load(cf)
    config.update(config_file)
    return copy(config)


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
        for p in paths:
            if not os.path.exists(p):
                os.makedirs(p)

        log_name = choose_log(local_config['LOG_DIR'])
        if log_name is None:
            logging.info("No logs found to parse")
            return
        log_date = log_name[-11:-3] if log_name[-3:] == '.gz' else log_name[-8:]

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
    for p in paths:
        if not os.path.exists(p):
            os.makedirs(p)
    for p in paths:
        fls = os.listdir(p)
        [os.remove(os.path.join(p, f)) for f in fls]


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
    arg_parser = argparse.ArgumentParser(description='Log analyzer arguments')
    arg_parser.add_argument("--config", type=str, default='./config')
    args = arg_parser.parse_args()
    config_loc = get_config(args.config)
    main(config_loc, smoke_test=False)
