# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
This class sets up to update the connections database.
"""
import datetime
from hera_mc import cm_active, cm_utils, cm_sysdef
from hera_mc import cm_partconnect as CMPC
from . import util, cm_gsheet, upd_base
from argparse import Namespace

cm_req = Namespace(hpn=cm_sysdef.hera_zone_prefixes, pol='all',
                   at_date='now', exact_match=False, hookup_type=None)


class UpdateConnect(upd_base.Update):
    pols = ['E', 'N']
    NotFound = "Not Found"

    def __init__(self, script_type='connupd', script_path='default', verbose=True):
        super(UpdateConnect, self).__init__(script_type=script_type,
                                            script_path=script_path,
                                            verbose=verbose)
        self.active = None
        self.skipping = []

    def pipe(self, node_csv='n'):
        self.load_gsheet(node_csv)
        self.load_active()
        self.make_sheet_connections()
        self.compare_connections()
        self.add_missing_parts()
        self.add_missing_connections()
        self.add_partial_connections()
        self.add_different_connections()
        self.add_rosetta()
        self.finish()
        self.show_summary_of_compare()

    def get_hpn_from_col(self, col, key, header):
        return util.gen_hpn(col, self.gsheet.data[key][header.index(col)])

    def load_active(self):
        """
        Gets the hookup data from the hera_mc database.
        """
        self.active = cm_active.ActiveData()
        self.active.load_connections()

    def make_sheet_connections(self):
        """
        Go through all of the sheet and make cm_partconnect.Connections for comparison.

        self.gsheet.connections is set up identically to self.active.connections
        """
        self.gsheet.connections = {'up': {}, 'down': {}}  # To mirror cm_active
        for sant in self.gsheet.ants:
            previous = {}
            for pol in self.pols:
                gkey = '{}-{}'.format(sant, pol)
                node_num = self.gsheet.data[gkey][0]
                tab = self.gsheet.ant_to_node[sant]
                header = self.gsheet.header[tab]
                for i, col in enumerate(header):
                    if col not in cm_gsheet.hu_col.keys():
                        continue
                    if self.gsheet.data[gkey][i] is None:
                        continue
                    if col in ['Ant', 'Feed', 'SNAP'] and pol == self.pols[1]:
                        this = self.get_hpn_from_col(col, gkey, header)
                        if this != previous[col]:
                            raise ValueError("{} != {}".format(this, previous[col]))
                        continue
                    if col == 'Ant':  # Make station-antenna, antenna-feed
                        ant = self.get_hpn_from_col('Ant', gkey, header)
                        previous['Ant'] = ant
                        sta = self._sta_from_ant(ant)
                        feed = self.get_hpn_from_col('Feed', gkey, header)
                        previous['Feed'] = feed
                        # ... station-antenna
                        keyup = cm_utils.make_part_key(sta, 'A')
                        pku = 'GROUND'
                        if self._status_OK(keyup, pol, [ant]):
                            self._ugconn(keyup, pku, [sta, 'A', 'ground'], [ant, 'H', 'ground'])
                        # ... antenna-feed
                        keyup = cm_utils.make_part_key(ant, 'H')
                        pku = 'FOCUS'
                        if self._status_OK(keyup, pol, [ant, feed]):
                            self._ugconn(keyup, pku, [ant, 'H', 'focus'], [feed, 'A', 'input'])
                    elif col == 'Feed':  # Make feed-fem
                        feed = self.get_hpn_from_col('Feed', gkey, header)
                        fem = self.get_hpn_from_col('FEM', gkey, header)
                        keyup = cm_utils.make_part_key(feed, 'A')
                        pku = 'TERMINALS'
                        if self._status_OK(keyup, pol, [feed, fem]):
                            self._ugconn(keyup, pku, [feed, 'A', 'terminals'], [fem, 'A', 'input'])
                    elif col == 'FEM':  # Make fem-nbp
                        fem = self.get_hpn_from_col('FEM', gkey, header)
                        nbp = util.gen_hpn('NBP', node_num)
                        port = '{}{}'.format(pol,
                                             self.gsheet.data[gkey][header.index('NBP/PAMloc')])
                        if port is not None:
                            port = port.lower()
                        keyup = cm_utils.make_part_key(fem, 'A')
                        if self._status_OK(keyup, pol, [fem, port]):
                            self._ugconn(keyup, pol, [fem, 'A', pol.lower()], [nbp, 'A', port])
                    elif col == 'NBP/PAMloc':  # nbp-pam
                        nbp = util.gen_hpn('NBP', node_num)
                        port = '{}{}'.format(pol,
                                             self.gsheet.data[gkey][header.index('NBP/PAMloc')])
                        if port is not None:
                            port = port.lower()
                            pku = port.upper()
                        pam = self.get_hpn_from_col('PAM', gkey, header)
                        if self._status_OK('-', pol, [port, pam]):
                            keyup = cm_utils.make_part_key(nbp, 'A')
                            self._ugconn(keyup, pku, [nbp, 'A', port], [pam, 'A', pol.lower()])
                    elif col == 'PAM':  # pam-snap
                        pam = self.get_hpn_from_col('PAM', gkey, header)
                        snap = self.get_hpn_from_col('SNAP', gkey, header)
                        previous['SNAP'] = snap
                        port = self.gsheet.data[gkey][header.index('Port')]
                        if len(port) == 0:
                            port = None
                        if port is not None:
                            port = port.lower()
                            if port[0] != pol[0].lower():
                                msg = "{} port ({}) and pol ({}) don't match".format(snap,
                                                                                     port, pol)
                                raise ValueError(msg)
                        keyup = cm_utils.make_part_key(pam, 'A')
                        if self._status_OK(keyup, pol, [pam, snap, port]):
                            self._ugconn(keyup, pol, [pam, 'A', pol.lower()], [snap, 'A', port])
                    elif col == 'SNAP':  # snap-node, pam-pch, pch-node
                        # ... snap-node
                        snap = self.get_hpn_from_col('SNAP', gkey, header)
                        node = util.gen_hpn("Node", node_num)
                        loc = "loc{}".format(self.gsheet.data[gkey][header.index('SNAPloc')])
                        if self._status_OK('-', pol, [snap, node, loc]):
                            keyup = cm_utils.make_part_key(snap, 'A')
                            pku = 'RACK'
                            self._ugconn(keyup, pku, [snap, 'A', 'rack'], [node, 'A', loc])
                        if self.active is None:
                            continue
                        # ... pam-pch
                        pam = self.get_hpn_from_col('PAM', gkey, header)
                        pku = 'SLOT'
                        try:
                            keyup = cm_utils.make_part_key(pam, 'A')
                            pch = self.active.connections['up'][keyup][pku].downstream_part
                        except KeyError:
                            continue
                        slot = '{}{}'.format('slot',
                                             self.gsheet.data[gkey][header.index('NBP/PAMloc')])
                        if self._status_OK('-', pol, [pam, pch, slot]):
                            self._ugconn(keyup, pku, [pam, 'A', 'slot'], [pch, 'A', slot])
                        # ... pch-node
                        node = util.gen_hpn("Node", node_num)
                        if self._status_OK('-', pol, [pch, slot, node]):
                            keyup = cm_utils.make_part_key(pch, 'A')
                            pku = 'RACK'
                            self._ugconn(keyup, pku, [pch, 'A', 'rack'], [node, 'A', 'bottom'])

    def _ugconn(self, k, p, up, dn):
        self.gsheet.connections['up'].setdefault(k, {})
        self.gsheet.connections['up'][k][p] = CMPC.Connections()
        self.gsheet.connections['up'][k][p].connection(
            upstream_part=up[0], up_part_rev=up[1], upstream_output_port=up[2],
            downstream_part=dn[0], down_part_rev=dn[1], downstream_input_port=dn[2])

    def _status_OK(self, keyup, pol, list_to_check):
        if None in list_to_check:
            if self.verbose:
                print(f'skipping {keyup} {pol}', list_to_check)
                self.skipping.append(keyup)
            return False
        if keyup != '-':
            if pol == self.pols[1]:  # Make sure key already there
                if keyup not in self.gsheet.connections['up']:
                    print(self.gsheet.connections['up'][keyup])
                    raise ValueError("{} not present ({}).".format(keyup, pol))
            else:  # Make sure it is not there, then process
                if keyup in self.gsheet.connections['up']:
                    print(self.gsheet.connections['up'][keyup])
                    raise ValueError("{} already present ({}).".format(keyup, pol))
        return True

    def _sta_from_ant(self, ant):
        antnum = int(ant[1:])
        if antnum < 320:
            return 'HH{}'.format(antnum)
        else:
            raise ValueError("NEED TO ADD OUTRIGGERS")

    def add_rosetta(self):
        for t2u in ['missing', 'different']:
            t2u_attr = getattr(self, t2u)
            if not len(t2u_attr):
                continue
            self.hera.no_op_comment('Adding {} parts'.format(t2u))
            for key in t2u_attr:
                if not key.startswith('SNP'):
                    continue
                try:
                    conn = self.active.connections[key]['up']['RACK']
                    hname = "heraNode{}Snap{}".format(int(conn.downstream_part[1:]),
                                                      int(conn.downstream_input_port[-1]))
                    self.hera.add_part_rosetta(conn.upstream_part, hname, self.cdate, self.ctime)
                except KeyError:
                    with open('upd_connect.log', 'a') as fp:
                        print(f"Need to put {key} in active connections.", file=fp)

    def compare_connections(self, direction='gsheet-active'):
        """
        Step through all of the sheet Connections and make sure they are all there and the same.
        """
        pside = 'up'
        self.compare_direction = direction
        self.missing = {}
        self.partial = {}
        self.different = {}
        self.different_stop = {}
        self.same = {}
        if direction.startswith('g'):
            A = self.gsheet.connections[pside]
            B = self.active.connections[pside]
        else:
            A = self.active.connections[pside]
            B = self.gsheet.connections[pside]
        for gkey, gpts in A.items():
            if gkey not in B.keys():
                self.missing[gkey] = gpts
                continue
            for p, c in gpts.items():
                if p not in B[gkey].keys():
                    self.partial.setdefault(gkey, {})
                    self.partial[gkey][p] = c
                elif B[gkey][p] == c:
                    self.same.setdefault(gkey, {})
                    self.same[gkey][p] = c
                else:
                    self.different.setdefault(gkey, {})
                    self.different[gkey][p] = c
                    self.different_stop.setdefault(gkey, {})
                    self.different_stop[gkey][p] = B[gkey][p]

    def add_missing_parts(self):
        self.active.load_parts()
        missing_parts = set()
        for pc in self.missing.values():
            for conn in pc.values():
                key = cm_utils.make_part_key(conn.upstream_part, conn.up_part_rev)
                if key not in self.active.parts.keys():
                    missing_parts.add(key)
                key = cm_utils.make_part_key(conn.downstream_part, conn.down_part_rev)
                if key not in self.active.parts.keys():
                    missing_parts.add(key)
        self.missing_parts = list(missing_parts)
        if len(self.missing_parts):
            self.hera.no_op_comment('Adding missing parts')
            add_part_time_offset = self.now - datetime.timedelta(seconds=300)
            cdate = add_part_time_offset.strftime('%Y/%m/%d')
            ctime = add_part_time_offset.strftime('%H:%M')
        for part in self.missing_parts:
            self.update_counter += 1
            p = list(cm_utils.split_part_key(part))
            this_part = p + [upd_base.signal_chain.part_types[part[:3]], p[0]]
            self.hera.update_part('add', this_part, cdate=cdate, ctime=ctime)

    def add_missing_connections(self):
        if len(self.missing):
            self.hera.no_op_comment('Adding missing connections')
            self._modify_connections(self.missing, 'add', self.cdate, self.ctime)

    def add_partial_connections(self):
        if len(self.partial):
            self.hera.no_op_comment('Adding partial connections')
            self._modify_connections(self.partial, 'add', self.cdate, self.ctime)

    def add_different_connections(self):
        if len(self.different):
            self.hera.no_op_comment('Adding different connections')
            stop_conn_time_offset = self.now - datetime.timedelta(seconds=90)
            cdate = stop_conn_time_offset.strftime('%Y/%m/%d')
            ctime = stop_conn_time_offset.strftime('%H:%M')
            self._modify_connections(self.different_stop, 'stop', cdate, ctime)
            self._modify_connections(self.different, 'add', self.cdate, self.ctime)

    def _modify_connections(self, this_one, add_or_stop, cdate, ctime):
        for mod_conn in this_one.values():
            for conn in mod_conn.values():
                self.update_counter += 1
                up = [conn.upstream_part, conn.up_part_rev, conn.upstream_output_port]
                dn = [conn.downstream_part, conn.down_part_rev, conn.downstream_input_port]
                self.hera.update_connection(add_or_stop, up, dn, cdate=cdate, ctime=ctime)

    def show_summary_of_compare(self):
        print("\n---Summary---")
        print("Missing:  {}".format(len(self.missing)))
        print("Same:  {}".format(len(self.same)))
        print("Skipping:  {}".format(len(self.skipping)))
        print("Partial:  {}".format(len(self.partial)), end='   ')
        if len(self.partial):
            print("*****CHECK*****")
            for p in self.partial:
                print("\t{}".format(p))
        else:
            print()
        print("Different:  {}".format(len(self.different)), end='   ')
        if len(self.different):
            print("*****CHECK*****")
            for d in self.different:
                print("\t{}".format(d))
        else:
            print()
        print("Different_stop:  {}".format(len(self.different_stop)), end='   ')
        if len(self.different_stop):
            print("*****CHECK*****")
            for d in self.different_stop:
                print("\t{}".format(d))
        else:
            print()
