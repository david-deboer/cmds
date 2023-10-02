#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2023 David R DeBoer
# Licensed under the 2-clause BSD license.

"""Script to get notes by date."""

import argparse
from cmds import cm_active, cm_utils
from tabulate import tabulate
import datetime

dhelpstr = "Number of days (+/-) around date to look.\n"\
           "If >= 0, the days argument will just search on calendar days over that many days, e.g. 2023/09/15 ignoring time.\n"\
           "If < 0, the days argument will search for comments within that many (fractional) days from posting time."

ap = argparse.ArgumentParser()
ap.add_argument('-d', '--days', help=dhelpstr, type=float, default=1.0)
ap.add_argument('-s', '--sortby', help="Sort output by 'part' or 'date' first", choices=['part', 'date'], default='date')
cm_utils.add_date_time_args(ap)
args = ap.parse_args()

date = cm_utils.get_astropytime(args.date, args.format)

if args.days < 0.0:
    args.day = abs(args.days)
    caldate = None
else:
    args.days = int(args.days)
    caldate = []
    for i in range(-args.days, args.days+1):
        caldate.append((date.datetime + datetime.timedelta(days=i)).strftime('%Y%m%d'))

def date_check(posting):
    if caldate is None:
        return abs(posting - date.gps) < (args.days * 3600 * 24)
    if cm_utils.get_astropytime(posting, float_format='gps').datetime.strftime('%Y%m%d') in caldate:
        return True
    return False

active = cm_active.get_active(date, loading=['info'])

found = {}
for hpn, notes in active.info.items():
    for note in notes:
        diff = abs(note.posting_gpstime - date.gps)
        if date_check(note.posting_gpstime):
            note.date = cm_utils.get_astropytime(note.posting_gpstime, float_format='gps')
            nk, pk, rk = cm_utils.peel_key(note.hpn, 'NPR')
            if args.sortby == 'date':
                key = f"{note.date.datetime}{pk}{nk:04d}{note.comment}"
            else:
                key = f"{pk}{nk:04d}{note.date.datetime}{note.comment}"
            found[key] = note

table_header = ['Date', 'PN', 'Note']
table_data = []
for this in sorted(found.keys()):
    table_data.append([found[this].date.datetime, found[this].pn, found[this].comment])

print(tabulate(table_data, headers=table_header))
