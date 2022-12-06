# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import random
from collections import defaultdict
from heapq import nlargest

import luigi
import luigi.contrib.hdfs
import luigi.contrib.postgres
import luigi.contrib.spark

class PriceStat(luigi.contrib.spark.SparkSubmitTask):
    app = 'get_price_stat.py'
    master = 'yarn'

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("data/price_stat/*")

    def app_options(self):
        return [self.output().path.rstrip('*')]


class OkDem(luigi.contrib.spark.SparkSubmitTask):
    app = 'get_ok_dem.py'
    master = 'yarn'

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("data/ok_dem/*")

    def requires(self):
        return PriceStat()

    def app_options(self):
        return [self.input().path, self.output().path.rstrip('*')]

class ProductStat(luigi.contrib.spark.SparkSubmitTask):
    app = 'get_product_stat.py'
    master = 'yarn'

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget("data/product_stat/*")

    def requires(self):
        return OkDem()

    def app_options(self):
        return [self.input().path, self.output().path.rstrip('*')]


if __name__ == "__main__":
    luigi.run()

