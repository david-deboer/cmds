# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
"""
import datetime
import argparse
import csv

new_lines_replace = ['Feed Height Before',
                     'Sticker Targets',
                     'Stiffener Braces',
                     'Carey Caps',
                     'Protected Fiber',
                     'Cable Conduit',
                     'Spiral Wrap',
                     'D Shackle Cable',
                     'Cables Labelled']


def preproc_h6c(line, newlines=new_lines_replace):
    for rnl in newlines:
        line = line.replace(rnl, '\n' + rnl)
    return line


def include_this_line_in_log(line, included):
    if '--date' not in line:
        return False
    trunc_line = line.split('--date')[0]
    for included_line in included:
        if included_line.startswith(trunc_line):
            return False
    return True


def parse_log_line(line,
                   arglist=['hpn', 'hptype', 'comment',
                            'uppart', 'upport',
                            'dnpart', 'dnport', 'status'],
                   prefix=None,
                   return_cmd_args = False,
                   **kwargs):
    """
    Parse an input line to produce a log output.  To add, include the argparse
    in the script_info.py file and include desired options to arglist above.
    """
    from . import script_info
    from hera_mc import cm_utils

    entry_type = {'add_connection': 'short',
                  'stop_connection': 'short',
                  'add_part': 'short',
                  'stop_part': 'short',
                  'add_part_info': 'short',
                  'other': 'short'}
    entry_type.update(kwargs)

    argv = list(csv.reader([line], delimiter=' '))[0]
    command = argv[0].split('.')[0]

    try:
        parser = getattr(script_info, command)()
    except AttributeError:
        return line + '\n'

    args = parser.parse_args(argv[1:])
    if return_cmd_args:
        return command, args
    helpdict = {}
    for action in parser._actions:
        helpdict[action.dest] = action.help
    dt = cm_utils.get_astropytime(args.date, args.time)

    if prefix is None:
        prefix = ' '.join(command.split('_'))

    if command in entry_type:
        use_etype = command
        # Temporary "H6C" processing:
        if command == 'add_part_info' and args.comment.lower().startswith('h6c'):
            entry_type[use_etype] = 'long'
            args.comment = preproc_h6c(args.comment)
    else:
        use_etype = 'other'
    if entry_type[use_etype] == 'short':
        ret = f'{prefix}: '
        for atr, hlp in helpdict.items():
            if atr in arglist:
                if atr in ['comment', 'status']:
                    delim = '"'
                else:
                    delim = ''
                param = getattr(args, atr)
                ret += f"  {helpdict[atr]}={delim}{param}{delim}"
        ret += f"  {dt.datetime.isoformat(timespec='seconds')}\n"
    elif entry_type[use_etype] == 'long':
        ret = f"-- {prefix} -- {dt.datetime.isoformat(timespec='seconds')}\n"
        for atr, hlp in helpdict.items():
            if atr in arglist:
                param = getattr(args, atr)
                ret += f"\t{helpdict[atr]}:  {param}\n"
    else:
        ret = line
    return ret


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


def gen_hpn(ptype, pnum, verbose=False):
    """
    From the sheet data (via ptype, pnum) etc it will generate a HERA Part Number
    """
    if pnum is None:
        return None
    ptype = ptype.upper()
    if isinstance(pnum, str):
        pnum = pnum.upper()
    try:
        number_part = int(get_num(pnum))
    except ValueError:
        return None
    if ptype in ['NBP/PAMloc', 'SNAPloc']:
        return number_part
    if ptype in ['SNP', 'SNAP']:
        snpletter = 'C'
        try:
            _ = int(pnum)
        except ValueError:
            snpletter = pnum[0]
        return f'SNP{snpletter}{number_part:06d}'
    if ptype in ['PAM', 'FEM']:
        return f'{ptype}{number_part:03d}'
    if ptype in ['NODESTATION']:
        return f'ND{number_part:02d}'
    if ptype in ['NODE', 'ND']:
        return f'N{number_part:02d}'
    if ptype in ['NBP']:
        return f'NBP{number_part:02d}'
    if ptype in ['FEED', 'FDV']:
        return f'FDV{number_part}'
    if ptype in ['ANT', 'ANTENNA']:
        return f'A{number_part}'
    if ptype in ['STATION']:
        from hera_mc.geo_sysdef import region
        if number_part in region['heraringa']:
            pre = 'HA'
        elif number_part in region['heraringb']:
            pre = 'HB'
        elif number_part > 9999:
            pre = 'EE'
        else:
            pre = 'HH'
        return f'{pre}{number_part}'
    if ptype in ['FPS']:
        return f'FPS{number_part:02d}'
    if ptype in ['PCH']:
        return f'PCH{number_part:02d}'
    if ptype in ['NCM']:
        return f'NCM{number_part:02d}'
