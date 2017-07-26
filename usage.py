#!/usr/bin/env python

from __future__ import print_function
import os
from datetime import datetime
import calendar
import itertools
import requests
from slipstream.api import Api

# Compute the following usage aggregation:
# - group by all input user, all measurements for all input clouds
# - group by all input cloud, all measurements for all input users
# a) complete months (from given range)
# b) complete weeks (this month)

#
# Inputs
#

year = 2017
months_range = range(6, 8)  # range(1, 13) ## for the entire year

clouds = {"exoscale-ch-gva", "exoscale-ch-dk", "open-telekom-de1"}
#organizations = {"CERN", "DESY", "CNRS", "INFN", "EMBL", "IFAE", "ESRF", "KIT", "STFC", "SURFSara"}
organizations = {"CERN", "DESY", "CNRS", "INFN", "EMBL", "IFAE", "KIT", "SURFSara"}

#
# Constants
#

username = os.environ['SLIPSTREAM_USERNAME']
password = os.environ['SLIPSTREAM_PASSWORD']

filename_template = "metrics--{year}-{month}--{organization}--{detail}.csv"

url_usage = "https://nuv.la/api/usage"

body_template_base = "frequency='{frequency}' " \
                     "and start-timestamp>={year}-{month_start}-01 " \
                     "and end-timestamp<={year}-{month_end}-01"

url_login = "https://nuv.la/auth/login"

cookies = None

#
# Functions
#


def _build_request_data_query(users, clouds):
    users_query_fragment = " or ".join(map(lambda x: "user='%s'" % x.username, users))
    clouds_query_fragment = " or ".join(map(lambda x: "cloud='%s'" % x, clouds))
    body = body_template_base + " and (%s) and (%s)" % (users_query_fragment, clouds_query_fragment)    
    return body


def build_request_data(frequency, month_start, users, clouds):
    return {"$filter":_build_request_data_query(users,clouds).format(frequency=frequency,
                                                                     month_start=month_start,
                                                                     month_end=get_month_end(month_start),
                                                                     year=year)}


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
    metrics = {}  # 1 entry per group_by
    for entry in resp['usages']:
        group_by_value = entry[group_by]
        include_all_value = entry[include_all]
        # print("grouping by %s = %s" % (group_by, group_by_value))
        metrics[group_by_value] = metrics.get(group_by_value,
                                              {include_all: set()})
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


def get_metrics(url, data):
    print('        Getting metrics with url: %s' % url)
    print('        and body: %s' % data)
#    data = {'$filter': "frequency='monthly' and start-timestamp>=2017-6-01 and end-timestamp<=2017-7-01 and (user='meb')"}
    response = requests.put(url, data=data, cookies=cookies)
#    print(response.json())
    test_response_raise(response, "Error getting usage: " + response.text)
    return response.json()


def convert(metric, value):
    if 'ram' == metric:
        return value / 60 / 1024  # return GB-hour from MB-minutes
    if 'disk' == metric:
        return value / 60  # / 1024 # return GB-hour from GB-minutes
    try:
        return value / 60  # return per minute??!
    except(TypeError):
        return value


def format(clouds_metrics, group_by, include_all, filename):
    columns = ["vm", "cpu", "ram", "disk", include_all]
    if not clouds_metrics:
        print("        >>> Warning... no metrics found!")
        return
    with open(filename, 'w') as f:
        f.write("%s, VM (hours), CPU (hours), RAM (GB-hours), "
                "Disk (GB-hours), Included %ss\n" % (group_by.title(),
                                                     include_all.title()))
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


def process_for_month(frequency, month_start, organization, users, clouds):
    data = build_request_data(frequency, month_start, users, clouds)
    #print("data:", data)
    response = get_metrics(url_usage, data)
    # First reduce for all users, then for all clouds
    for group_by, include_all in [["user", "cloud"], ["cloud", "user"]]:
        clouds_metrics = reduce(response, group_by, include_all)
        filename = filename_template.format(
            year=year,
            month=pad_filename(month_start),
            organization=organization,
            detail="by-%s" % group_by)
        format(clouds_metrics, group_by, include_all, filename)


def process_usage(organization, users, clouds):
    _months_range = months_range
    this_month = datetime.now().month
    use_weekly = True if this_month == months_range[-1] else False

    if use_weekly:
        _months_range = months_range[:-1]

    print("users",users)

    print('Processing usage for organization %s, including the following users:' % organization)
    map(print,["    - %s %s %s" % (u.first_name,
                                   u.last_name,
                                   (u.username[:50] + '...') if len(u.username) > 50 else u.username)
               for u in users])

    frequency = 'monthly'
    for m in _months_range:
        print('    Processing for month: %s...' % (calendar.month_name[m]))
        frequency = 'monthly'
        process_for_month(frequency, m, organization, users, clouds)

    if use_weekly:
        # Process last month as weekly since we don't have a complete month
        m = months_range[-1]
        print('    Processing current month using weekly summary: %s...' % (calendar.month_name[m]))
        frequency = 'weekly'
        process_for_month(frequency, m, organization, users, clouds)


def merge_files():
    filenames = map(lambda m: 'metrics-%s.csv' % pad_filename(m), months_range)
    with open('metrics.csv', 'w') as o:
        for filename in filenames:
            with open(filename) as infile:
                o.write('filename: %s\n' % filename)
                o.write(infile.read())

def get_users_by_organisation(all_users, organizations):
    users_by_org = {}
    for o in organizations:
        users_by_org[o] = filter(lambda u: u.organization == o, all_users)
    return users_by_org

def get_all_users(api):
    api.login(username, password)
    return list(api.list_users())

def extract_organizations(users):
    return sorted(set(map(lambda u: u.__getattribute__("organization"), users)))

def get_all_filtered_users(users_by_org):
    all_filtered_users = list(itertools.chain(users_by_org.values()))
    all_filtered_users_flattened = [val for sublist in all_filtered_users for val in sublist]
    return all_filtered_users_flattened
    
def main():
    api = Api()
    all_users = get_all_users(api)
    
    # Check that the orgaisations we have defined exists (i.e. users have declared being part of them)
    orgs = extract_organizations(all_users)
    unknown_org = organizations.difference(orgs)
    if unknown_org:
        print(">>> Error, the following organizations are not used by any user:", ", ".join(unknown_org))
        return
    
    users_by_org = get_users_by_organisation(all_users, organizations)

    print("Processing usage for clouds:", ", ".join(clouds))

    for org in users_by_org:
        process_usage(org, users_by_org[org], clouds)

    all_filtered_users = get_all_filtered_users(users_by_org)

    # All users from all organizations
    process_usage("all", all_filtered_users, clouds)
    
        
cookies = login()
main()
