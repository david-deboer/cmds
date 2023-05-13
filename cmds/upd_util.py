# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
"""
import datetime
import argparse


def as_part(add_or_stop, p, cdate, ctime):
    """Return a string to use cmds script to add or stop a part."""
    s = f'cmds_update_part.py {add_or_stop} {p[0]} '
    if add_or_stop == 'add':
        s += f'-t {p[1]} -m {p[2]} '
    s += f'--date {cdate} --time {ctime}'
    return s


def as_connect(add_or_stop, up, dn, cdate, ctime):
    """Return a string to use cmds script to add or stop a connection."""
    s = 'cmds_update_connection.py {} -u {} --upport {} -d {} --dnport {}'\
        ' --date {} --time {}'.format(add_or_stop, up[0], up[1],
                                        dn[0], dn[1], cdate, ctime)
    return s


def include_this_line_in_log(line, included):
    if '--date' not in line:
        return False
    trunc_line = line.split('--date')[0]
    for included_line in included:
        if included_line.startswith(trunc_line):
            return False
    return True


def YMD_HM(dt, offset=0.0, add_second=False):
    dt += datetime.timedelta(offset)
    if add_second:
        return dt.strftime('%Y/%m/%d'), dt.strftime('%H:%M:%S')
    else:
        return dt.strftime('%Y/%m/%d'), dt.strftime('%H:%M')


def compare_lists(list1, list2, info=None, ignore_length=True):
    """
    Make sure the lists agree.  If ignore_length, then check
    as far as length of list1
    """
    if not ignore_length:
        if len(list1) != len(list2):
            print("List lengths differ")
        return False

    are_same = True
    for i, l1 in enumerate(list1):
        if l1 != list2[i]:
            if info is not None:
                print(info)
            print("\t{} and {} are not the same".format(l1, list2[i]))
            are_same = False
    return are_same


def get_num(val):
    """
    Makes digits in alphanumeric string into a number as string
    """
    if isinstance(val, (int, float)):
        return str(val)
    return ''.join(c for c in val if c.isnumeric())


def get_bracket(input_string, bracket_type='{}'):
    """
    Breaks out stuff as <before, in, after>.
    If no starting bracket, it returns <None, input_string, None>
    Used in parse_stmt below.
    """
    start_ind = input_string.find(bracket_type[0])
    if start_ind == -1:
        return None, input_string, None
    end_ind = input_string.find(bracket_type[1])
    prefix = input_string[:start_ind].strip()
    statement = input_string[start_ind + 1: end_ind].strip()
    postfix = input_string[end_ind + 1:].strip()
    return prefix, statement, postfix


def parse_command_payload(col):
    """
    Parses the full command payload.
    """
    prefix, stmt, postfix = get_bracket(col, '{}')
    isstmt = prefix is not None
    edate = False
    prefix, entry, postfix = get_bracket(stmt, '[]')
    if prefix is not None:
        edate = entry
        entry = prefix
    return argparse.Namespace(isstmt=isstmt, entry=entry, date=edate)


def get_unique_pkey(hpn, rev, pdate, ptime, old_timers):
    """
    Generate unique info_pkey by advancing the time tag a second at a time if needed.
    """
    if ptime.count(':') == 1:
        ptime = ptime + ':00'
    pkey = '|'.join([hpn, rev, pdate, ptime])
    while pkey in old_timers:
        newdt = datetime.datetime.strptime('-'.join([pdate, ptime]),
                                           '%Y/%m/%d-%H:%M:%S') + datetime.timedelta(seconds=2)
        pdate = newdt.strftime('%Y/%m/%d')
        ptime = newdt.strftime('%H:%M:%S')
        pkey = '|'.join([hpn, rev, pdate, ptime])
    return pkey, pdate, ptime


def get_row_dict(hdr, data):
    """
    Makes a dictionary providing mapping of column headers and column numbers to data.
    """
    row = {}
    for i, h in enumerate(hdr):
        row[h] = data[i]
        row[i] = data[i]
    return row
