#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Script to run the hookup check between gsheet and database."""

import argparse
from cmds import upd_connect
from os import path

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('-d', '--direction', choices=['active-gsheet', 'gsheet-active'],
                    default='gsheet-active',
                    help="Compare A vs B, missing means present in A and not B.")
    ap.add_argument('--script-path', dest='script_path', help="Path for active script",
                    default='default')
    ap.add_argument('--archive-path', dest='archive_path', help="Path for script archive.",
                    default='___cm_updates')
    ap.add_argument('--arc_csv', help="For testing: flag for read/write of gsheet (r/w/n)",
                    choices=['read', 'write', 'none', 'r', 'w', 'n'], default='n')
    ap.add_argument('-v', '--verbose', help="Turn verbosity on.", action='store_true')
    args = ap.parse_args()
    cron_script = 'conn_update.sh'
else:
    args = argparse.Namespace(archive_path=None, script_path='./', node_csv='n', verbose=True)
    print(args)
    cron_script = None

script_type = 'connupd'
cronjob_script = 'conn_update.sh'

update = upd_connect.UpdateConnect(script_type=script_type,
                                   script_path=args.script_path,
                                   verbose=args.verbose)

if args.archive_path.startswith('___'):
    args.archive_path = path.join(update.script_path, args.archive_path[3:])
if args.archive_gsheet.startswith('___'):
    args.archive_gsheet = path.join(update.script_path, args.archive_gsheet[3:])

update.load_gsheet(split=False, arc_csv=args.arc_csv, path=args.archive_gsheet, time_tag=args.time_tag)
update.make_sheet_connections()
update.compare_connections(args.direction)
update.add_missing_parts()
update.add_missing_connections()
update.add_partial_connections()
update.add_different_connections()
update.finish(cronjob_script=cronjob_script, archive_to=args.archive_path)
