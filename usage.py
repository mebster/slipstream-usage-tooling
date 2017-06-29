#!/usr/bin/env python

from __future__ import print_function
import os
import calendar
import requests

username = os.environ['SLIPSTREAM_USERNAME']
password = os.environ['SLIPSTREAM_PASSWORD']

moth_range = range(1, 13)
clouds = ["ifb-bird-stack", "ifb-bistro-iphc", "ifb-core-pilot",
          "ifb-genouest-genostack", "ifb-prabi-girofle"]

url_template = "https://nuv.la/api/usage?$filter=cloud='{cloud}' and " \
               "frequency='monthly' and start-timestamp=2017-{month}-01"
url_login = "https://nuv.la/auth/login"

cookies = None


def test_response_raise(response, message):
    if response.status_code != requests.codes.ok:
        raise ValueError(message)


def login():
    r = requests.post(url_login, data={"username": username,
                                       "password": password})
    test_response_raise(r, "Unauthorized")
    return r.cookies


def process_respose(resp):
    metrics = {}
    users = set()
    for entry in resp['usages']:
        users.add(entry['user'])
        for metric_name, metrics_value in entry['usage'].iteritems():
            for k, v in metrics_value.iteritems():
                metric = metrics.get(metric_name, {k: 0})
                metric[k] = metric[k] + v
                metrics[metric_name] = metric
    metrics['users'] = users
    return metrics


def cloud_loop(month):
    clouds_metrics = {}
    for cloud in clouds:
        print('   Processing cloud:', cloud)
        url = url_template.format(cloud=cloud, month=month)
        print('   with url: %s' % url)
        response = requests.get(url, cookies=cookies)
        test_response_raise(response, "Error getting usage: " + response.text)
        clouds_metrics[cloud] = process_respose(response.json())
    return clouds_metrics


def convert(metric, value):
    if 'ram' == metric:
        return value / 60 / 1024  # return GB-hour from MB-minutes
    if 'disk' == metric:
        return value / 60  # / 1024 # return GB-hour from GB-minutes
    return value / 60


def format(clouds_metrics, filename):
    columns = ["vm", "cpu", "ram", "disk"]
    with open(filename, 'w') as f:
        f.write("Cloud, VM (hours), CPU (hours), RAM (GB-hours), "
                "Disk (GB-hours), Active users\n")
        for cm in clouds_metrics:
            f.write("%s, " % cm)
            foundit = False
            for c in columns:
                for m in clouds_metrics[cm]:
                    if c == m:
                        value = clouds_metrics[cm][m][u'unit-minutes']
                        f.write("%s, " % convert(c, value))
                        foundit = True
                        break
                if foundit is not True:
                    f.write(", ")
            f.write(" ".join(clouds_metrics[cm]['users']))
            f.write("\n")


def pad_filename(f):
    return str(f).zfill(2)


def months_loop():
    for m in moth_range:
        print('Processing for month: %s...' % calendar.month_name[m])
        clouds_metrics = cloud_loop(m)
        format(clouds_metrics, "metrics-%s.csv" % pad_filename(m))


def merge_files():
    filenames = map(lambda m: 'metrics-%s.csv' % pad_filename(m), moth_range)
    with open('metrics.csv', 'w') as o:
        for filename in filenames:
            with open(filename) as infile:
                o.write('filename: %s\n' % filename)
                o.write(infile.read())

cookies = login()
months_loop()
merge_files()
