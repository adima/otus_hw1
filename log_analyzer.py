#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import re
# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
#                     '$request_time';

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
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
        pass
    else:
        return sorted(fls)[-1]

def parse_log(log_dir, log_name, smoke_test=False):
    # 'nginx-access-ui.log-20170630'
    with io.open(os.path.join(log_dir, log_name)) as f:
        #     stats_url_ct = Counter()
        stats_url_time_sum = dict()
        line_ct = 0
        req_time_total = 0
        for n, l in enumerate(f):
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
    #         stats_url_time_sum[url] += req_time
    
    result = [{'url': key,
               'count': len(value),
               'count_perc': len(value) / float(line_ct),
               'time_sum': sum(value),
               'time_perc': sum(value) / req_time_total,
               'time_avg': sum(value) / len(value),
               'time_max': max(value),
               'time_median': median(value),
               }
              for key, value in stats_url_time_sum.items()]
    return result
    


def main():
    parse_log(log_dir='.', log_name='nginx-access-ui.log-20170630', smoke_test=True)

if __name__ == "__main__":
    main()
