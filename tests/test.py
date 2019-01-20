#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
import unittest

from src.log_analyzer import main, get_config, check_and_clear_test_folders, choose_log


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


if __name__ == '__main__':
    unittest.main()
