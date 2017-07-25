#!/usr/bin/env python

from __future__ import print_function
import os
from datetime import datetime
import calendar
import requests

# Compute the following usage aggregation:
# - group by all input user, all measurements for all input clouds
# - group by all input cloud, all measurements for all input users
# a) complete months (from given range)
# b) complete weeks (this month)

#
## Inputs
#

year = 2017
months_range = range(6, 8) # range(1, 13) ## for the entire year

clouds = ["exoscale-ch-gva", "exoscale-ch-dk", "open-telekom-de1"]
users = ["meb", "test"]
# clouds = ["exoscale-ch-gva"]
# users = ["meb"]

#
## Constants
#

username = os.environ['SLIPSTREAM_USERNAME']
password = os.environ['SLIPSTREAM_PASSWORD']

filename_template = "metrics--{year}-{month}--{detail}.csv"

url_template_base = "https://nuv.la/api/usage?$filter=frequency='{frequency}' " \
                    "and start-timestamp>={year}-{month_start}-01 " \
                    "and end-timestamp<={year}-{month_end}-01"

body_template = "..."

url_login = "https://nuv.la/auth/login"

cookies = None

#
## Functions
#


def _build_query(users, clouds):
    users_query_fragment = " or ".join(map(lambda x: "user='%s'" % x, users))
    clouds_query_fragment = " or ".join(map(lambda x: "cloud='%s'" % x, clouds))
    query = url_template_base + " and (%s) and (%s)" % (users_query_fragment, clouds_query_fragment)    
    return query


def build_query(frequency, month_start):
    return _build_query(users,clouds).format(frequency=frequency,
                                      month_start=month_start,
                                      month_end=get_month_end(month_start),
                                      year=year)


def get_month_end(start):
    return 1 if start >= 12 else start+1 


def test_response_raise(response, message):
    if response.status_code != requests.codes.ok:
        raise ValueError(message)


def login():
    r = requests.post(url_login, data={"username": username,
                                       "password": password})
    test_response_raise(r, "Unauthorized")
    return r.cookies


def reduce(resp, group_by, include_all):
    # For example: group_by=user, include_all=cloud
    # each entry (line at the end) correspond to a user, aggregating all clouds
    # return a set, with each entry corresponding to the group_by field
    metrics = {} # 1 entry per group_by
    for entry in resp['usages']:
        group_by_value = entry[group_by]
        include_all_value = entry[include_all]
        #print("grouping by %s = %s" % (group_by, group_by_value))
        metrics[group_by_value] = metrics.get(group_by_value, {include_all: set()})
        metrics[group_by_value][include_all].add(include_all_value)
        for metric_name, metrics_value in entry['usage'].iteritems():
            for k, v in metrics_value.iteritems():
                metric = metrics[group_by_value].get(metric_name, {k: 0})
                metric[k] = metric[k] + v
                metrics[group_by_value][metric_name] = metric
    # pretty print the set of include_all
    for m in metrics:
        metrics[m][include_all] = ", ".join(metrics[m][include_all])
    return metrics


def get_metrics(url):
    print('   Getting metrics with url: %s' % url)
    response = requests.get(url, cookies=cookies)
    test_response_raise(response, "Error getting usage: " + response.text)
    return response.json()


def convert(metric, value):
    if 'ram' == metric:
        return value / 60 / 1024  # return GB-hour from MB-minutes
    if 'disk' == metric:
        return value / 60  # / 1024 # return GB-hour from GB-minutes
    try:
        return value / 60 # return per minute??!
    except(TypeError):
        return value


def format(clouds_metrics, group_by, include_all, filename):
    columns = ["vm", "cpu", "ram", "disk", include_all]
    with open(filename, 'w') as f:
        f.write("%s, VM (hours), CPU (hours), RAM (GB-hours), "
                "Disk (GB-hours), Included %ss\n" % (group_by.title(), include_all.title()))
        for group in clouds_metrics:
            f.write("%s, " % group)
            for c in columns:
                cm = clouds_metrics[group][c]
                if isinstance(cm, dict):
                    value = cm[u'unit-minutes']
                else:
                    value = cm
                f.write("%s" % convert(c, value))
                if c != columns[-1]:
                    f.write(", ")
            f.write("\n")


def pad_filename(f):
    return str(f).zfill(2)


def process_for_month(frequency, month_start):
    url = build_query(frequency, month_start)
    response = get_metrics(url)
    # First reduce for all users, then for all clouds
    for group_by, include_all in [["user", "cloud"], ["cloud", "user"]]: # group_by, include_all
        clouds_metrics = reduce(response, group_by, include_all)
        filename = filename_template.format(
            year=year,
            month=pad_filename(month_start),
            detail="by-%s" % group_by)
        format(clouds_metrics, group_by, include_all, filename)


def processing_loop():
    _months_range = months_range
    this_month = datetime.now().month
    use_weekly = True if this_month == months_range[-1] else False

    if use_weekly:
        _months_range = months_range[:-1]

    frequency = 'monthly'
    for m in _months_range:
        print('Processing for month: %s...' % (calendar.month_name[m]))
        frequency = 'monthly'
        process_for_month(frequency, m)

    if use_weekly:
        # Process last month as weekly, assuming it is not complete
        m = months_range[-1]
        print('Processing current month using weekly summary: %s...' % (calendar.month_name[m]))
        frequency = 'weekly'
        process_for_month(frequency, m)


# def merge_files():
#     filenames = map(lambda m: 'metrics-%s.csv' % pad_filename(m), months_range)
#     with open('metrics.csv', 'w') as o:
#         for filename in filenames:
#             with open(filename) as infile:
#                 o.write('filename: %s\n' % filename)
#                 o.write(infile.read())

# Compute the following usage aggregation:
# - for all given users, all given clouds
# - for all given clouds, all given users
# - for all given user and cloud
# a) complete months (from given range)
# b) complete weeks (this month)

cookies = login()
processing_loop()
#merge_files()
