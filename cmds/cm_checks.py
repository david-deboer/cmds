# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Series of database checks."""
from . import cm_utils, cm_active, cm


def _notsame(a, b, **kwargs):
    params = {'ignore_case': True, 'ignore_no_data': 1}
    for key, val in kwargs.items():
        params[key] = val
    if params['ignore_case']:
        a = a.lower()
        b = b.lower()
    if params['ignore_no_data'] and (a == '-' and b == '-'):  # no data for either
        return False
    if params['ignore_no_data'] > 1 and (a == '-' or b == '-'):  # no data for one
        return False
    if a == 'x' or b == 'x':  # one doesn't have access
        return False
    if a == b:
        return False
    return True


class Checks:
    """Check class."""

    def __init__(self, start_time=2458500, stop_time='now', day_step=1.0):
        """Initialize."""
        self.session = cm.CMSessionWrapper()
        self.active = cm_active.ActiveData(self.session)
        self.start = cm_utils.get_astropytime(start_time, float_format='jd')
        self.stop = cm_utils.get_astropytime(stop_time, float_format='jd')
        self.step = day_step
        self.chk_same = None

    def info_log(self, look_back=7.0, outfile='info_log.csv'):
        """
        Get all info comments within look_back time (days).
        """
        from cmds import cm_hookup
        print(f"Writing log of last {look_back} days to {outfile}")
        import csv
        look_gps = cm_utils.get_astropytime('now').gps - look_back * 3600 * 24
        self.active.load_info()
        fnd = {}
        hookup = cm_hookup.Hookup()
        for hpn, data in self.active.info.items():
            found_one_yet = False
            for entry in data:
                if entry.posting_gpstime >= look_gps:
                    if not found_one_yet:  # Get node
                        found_one_yet = True
                        xhpn, xrev = cm_utils.split_part_key(hpn)
                        xx = hookup.get_hookup(xhpn)
                        try:
                            node = xx[hpn].hookup.popitem()[-1][-1].downstream_part
                        except:  # noqa
                            node = 'N/A'
                    key = f"{entry.posting_gpstime}:{hpn}"
                    datet = cm_utils.get_astropytime(entry.posting_gpstime, float_format='gps')
                    fnd[key] = [hpn, node, entry.comment, datet.isot]

        with open(outfile, 'w') as fp:
            writer = csv.writer(fp)
            for key in sorted(fnd.keys(), reverse=True):
                writer.writerow(fnd[key])

    def for_same(self, sep=',', **kwargs):
        """
        use sep='\t' for pretty and ',' for csv
        """
        if self.chk_same is None:
            print("Run 'check_hosts_ethers' first.")
            return
        for key, data in self.chk_same.items():
            for dev in ['arduino', 'wr', 'snap0', 'snap1', 'snap2', 'snap3']:
                for id in ['serial', 'mac', 'ip']:
                    for _i in range(len(data['source']) - 1):
                        for _j in range(_i + 1, len(data['source'])):
                            if _notsame(data[dev][id][_i], data[dev][id][_j], **kwargs):
                                print(f"{key}-{dev}-{id}", end=sep)
                                print("{}|{}{}{}{}!={}{}".format(data['source'][_i],
                                                                 data['source'][_j], sep,
                                                                 data[dev][id][_i], sep, sep,
                                                                 data[dev][id][_j]))

    def duplicate_comments(self, verbose=False):
        """Check the database for duplicate comments."""
        cmdpre = 'delete from part_info where hpn='
        filename = 'dupcomm.sql'
        self.active.load_info()
        duplicates_found = 0
        with open(filename, 'w') as fp:
            for part, comments in self.active.info.items():
                ncomm = len(comments)
                for i in range(ncomm - 1):
                    for j in range(i + 1, ncomm):
                        if comments[i].comment == comments[j].comment:
                            duplicates_found += 1
                            hpn, rev = cm_utils.split_part_key(part)
                            if comments[i].posting_gpstime > comments[j].posting_gpstime:
                                posting_gpstime = comments[i].posting_gpstime
                            else:
                                posting_gpstime = comments[j].posting_gpstime
                            print("{}'{}' and hpn_rev='{}' and posting_gpstime='{}';"
                                  .format(cmdpre, hpn, rev, posting_gpstime), file=fp)
                            if verbose:
                                print(f"{hpn} ({posting_gpstime}): {comments[i]}")
        if duplicates_found:
            print("{} duplicates found".format(duplicates_found))
            print("run 'psql XXX -f {}'".format(filename))
        else:
            print("No duplicates found.")

    def apriori(self):
        """Check for multiple apriori active states between start and stop times."""
        next = self.start
        while next < self.stop:
            print(next.isot)
            print("This doesn't do anything yet")
            self.active.load_apriori(next)
            next += self.step

    def part_conn_assoc(self):
        """
        Check to make sure that all connections have an associated active part.

        The database will allow non-active parts to have a connection, which is not
        desired.  This will check and list connections without an associated active
        part.  It does not Error or Warn, but just lists.

        Returns
        -------
        list
            List of missing parts.
        """
        missing_parts = {}
        next = self.start
        print("Starting check at {}".format(next.isot))
        while next < self.stop:
            self.active.load_parts(next)
            self.active.load_connections(next)
            full_part_set = list(self.active.parts.keys())
            full_conn_set = set(list(self.active.connections['up'].keys())
                                + list(self.active.connections['down'].keys()))
            for key in full_conn_set:
                if key not in full_part_set:
                    if key not in missing_parts:
                        print(self.active.at_date.isot)
                        print("\t{} is not listed as an active part "
                              "even though listed in an active connection.".format(key))
                        try:
                            print('\t', self.active.connections['up'][key])
                        except KeyError:
                            pass
                        try:
                            print('\t', self.active.connections['down'][key])
                        except KeyError:
                            pass
                    missing_parts.setdefault(key, [])
                    missing_parts[key].append(next.gps)
            next += self.step
        print("Stopping check at {}".format(next.isot))
        if len(missing_parts.keys()):
            print('Found the following parts:')
            for key in missing_parts.keys():
                print(f"\t{key}:  {missing_parts[key][0]}")
