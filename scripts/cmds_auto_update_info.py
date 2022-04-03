#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Script to run the info update between the googlesheet and database."""

import argparse
from hera_cm import upd_info
from os import path


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--script-path', dest='script_path', help="Path for active script",
                    default='default')
    ap.add_argument('--archive-path', dest='archive_path', help="Path for script archive.",
                    default='___cm_updates')
    ap.add_argument('-n', '--node_csv', help="For testing: flag for read/write of gsheet (r/w/n)",
                    choices=['read', 'write', 'none', 'r', 'w', 'n'], default='n')
    ap.add_argument('-v', '--verbose', help="Turn verbosity on.", action='store_true')
    ap.add_argument('-d', '--duplication_window', type=float,
                    help="Number of days to use for duplicate comments.", default=180.0)
    ap.add_argument('--view_duplicate', type=float,
                    help='In verbose, only show duplicates after this many days', default=0.0)
    ap.add_argument('--look_only', help='Flag to only look at data.', action='store_true')
    ap.add_argument('--time_tag', help='Flag to add time to node csv filename', action='store_true')
    ap.add_argument('--archive_gsheet', help='Path to move gsheet archive',
                    default='___cm_updates/gsheet')
    args = ap.parse_args()
else:
    args = argparse.Namespace(archive_path=None, script_path='default', node_csv='r', verbose=True,
                              duplication_window=70.0, view_duplicate=10.0, look_only=False)
    print(args)

if args.look_only:
    script_type = None
    cron_script = None
else:
    script_type = 'infoupd'
    cron_script = 'info_update.sh'

if args.time_tag:
    args.time_tag = '_%y%m%d'

update = upd_info.UpdateInfo(script_type=script_type,
                             script_path=args.script_path,
                             verbose=args.verbose)
if args.archive_path.startswith('___'):
    args.archive_path = path.join(update.script_path, args.archive_path[3:])
if args.archive_gsheet.startswith('___'):
    args.archive_gsheet = path.join(update.script_path, args.archive_gsheet[3:])
update.load_gsheet(node_csv=args.node_csv, path=args.archive_gsheet, time_tag=args.time_tag)
update.load_active()
update.load_gnodes()
update.add_apriori()
update.add_sheet_notes(duplication_window=args.duplication_window,
                       view_duplicate=args.view_duplicate)
update.add_gnodes(duplication_window=args.duplication_window,
                  view_duplicate=args.view_duplicate)
update.log_apriori_notifications()
if args.look_only:
    update.view_info()
else:
    update.finish(cron_script=cron_script, archive_to=args.archive_path)
