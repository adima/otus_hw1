#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import re
import gzip
import argparse
import json
import logging

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
    "ERRORS_THRSH": .8
}

def median(lst):
    sortedLst = sorted(lst)
    lstLen = len(lst)
    index = (lstLen - 1) // 2

    if (lstLen % 2):
        return sortedLst[index]
    else:
        return (sortedLst[index] + sortedLst[index + 1])/2.0

def choose_log(log_dir):
    fls = os.listdir(log_dir)
    fls = [f for f in fls if re.match(r'nginx-access-ui\.log-\d{6}(\.gz)?', f)]
    if len(fls) == 0:
        return
    else:
        return sorted(fls)[-1]


def parse_log(log_dir, log_name, report_size, errors_thrshold, smoke_test=False):
    reader = gzip if log_name[-2:] == 'gz' else io

    with reader.open(os.path.join(log_dir, log_name)) as f:
        stats_url_time_sum = dict()
        line_ct = 0
        req_time_total = 0
        errors_ct = 0
        for n, l in enumerate(f):
            try:
                line_ct += 1
                l_spl = l.strip().split(' ')
                url = l_spl[7]
                req_time = float(l_spl[-1])
                req_time_total += req_time
                # stats_url_ct[url] += 1
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


def main(smoke_test=False):
    if smoke_test:
        logging.info('Starting if smoke test mode')
    arg_parser = argparse.ArgumentParser(description='Log analyzer arguments')
    arg_parser.add_argument("--config", type=str, default='./config')
    args = arg_parser.parse_args()
    with io.open(args.config) as cf:
        config_file = json.load(cf)
    config.update(config_file)

    logging_path = config.get('logging_path')
    logging.basicConfig(filename=logging_path,
                        format='[%(asctime)s] %(levelname).1s %(message)s',
                        datefmt='%Y.%m.%d %H:%M:%S')
    logging.getLogger().setLevel(logging.DEBUG)

    try:
        paths = [config['LOG_DIR'], config['REPORT_DIR'], 'done']
        for p in paths:
            if not os.path.exists(p):
                os.makedirs(p)

        log_name = choose_log(config['LOG_DIR'])
        if log_name is None:
            logging.info("No logs found to parse")
            return
        log_date = log_name[-11:-3] if log_name[-3:] == '.gz' else log_name[-8:]

        result = parse_log(log_dir=config['LOG_DIR'],
                           log_name=log_name,
                           report_size=config['REPORT_SIZE'],
                           errors_thrshold=config['ERRORS_THRSH'],
                           smoke_test=smoke_test)

        rendered_temp = webpage_template.safe_substitute(dict(table_json=result))

        html_path = os.path.join(config['REPORT_DIR'], 'report-%s.%s.%s.html' % (log_date[:4],
                                                                                 log_date[4:6], log_date[6:8]))
        with io.open(html_path, 'w') as fh:
            fh.write(rendered_temp.decode('utf-8'))

        os.rename(os.path.join(config['LOG_DIR'], log_name),
                  os.path.join('done', log_name))
    except:
        logging.exception("Something unexpected happened")



if __name__ == "__main__":
    main(smoke_test=True)
