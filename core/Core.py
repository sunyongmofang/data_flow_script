#!/usr/bin/python

import os
from pyspark import SparkContext, SparkConf

class CoreClazz(object):

    def __init__(self, lv_date, log_date, param, split_str):
        self.lv_date, self.log_date, self.param = lv_date, log_date, param
        self.plugins = []
        self.ini_rdd(split_str)
        self.loadPlugins()
        self.is_debug = True

    def ini_rdd(self, split_str):
        appName = [self.log_date, "\t",self.param["appName"]]
        conf = SparkConf().setAppName("".join(appName))
        sc = SparkContext(conf = conf)
        fstext = sc.textFile(self.param["fs_path"]).map(lambda line:line.split(split_str)).filter(lambda line:len(line)>=43)
        bstext = sc.textFile(self.param["bs_path"]).map(lambda line:line.split(split_str)).filter(lambda line:len(line)>=58)
        bptext = sc.textFile(self.param["bp_path"]).map(lambda line:line.split(split_str)).filter(lambda line:len(line)>=6)
        self.fs_rdd = fstext
        self.bs_rdd = bstext
        self.bp_rdd = bptext
        self.sc = sc

    def loadPlugins(self):
        plugins_dir = self.param["plugins_dir"]
        for dir_t in self.param["plugins"]:
            dir_p = "/".join([plugins_dir, dir_t])
            for filename in os.listdir(dir_p):
                if filename.endswith(".py") and (not filename.startswith("_")):
                    self.getPluginsClazz(plugins_dir, dir_t, filename)

    def getPluginsClazz(self, plugins_dir, dir_t, filename):
        pluginName = os.path.splitext(filename)[0]
        import_path = [plugins_dir, dir_t, pluginName]
        plugin = __import__(".".join(import_path), fromlist = [pluginName])
        clazz = plugin.getPluginClass()
        o = clazz()
        o.setPlatform(self)
        self.plugins.append(o)

    def startover(self):
        for o in self.plugins:
            if not self.is_debug:
                try:
                    o.start()
                except Exception as e:
                    print e
            else:
                o.start()

    def shutdown(self):
        for o in self.plugins:
            if not self.is_debug:
                try:
                    o.stop()
                except Exception as e:
                    print e
            else:
                o.stop()
            o.setPlatform(None)
        self.plugins = []
        self.sc.stop()
