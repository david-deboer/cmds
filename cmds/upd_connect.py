# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""
This class sets up to update the connections database.
"""
from . import cm_tables, cm_sysdef
from . import upd_util, upd_base
import datetime
from copy import copy


class UpdateConnect(upd_base.Update):
    NotFound = "Not Found"

    def __init__(self, script_type='connupd', script_path='default', hookup_type='ata-rfsoc', verbose=True, **kwargs):
        """
        kwargs can contain:cm_config_path, cm_db_name
        """
        if not len(kwargs):
            args = None
        else:
            from argparse import Namespace
            args = Namespace(**kwargs)
        super(UpdateConnect, self).__init__(script_type=script_type,
                                            script_path=script_path,
                                            verbose=verbose,
                                            args=args)
        self.conn_track = {'add': [], 'stop': []}
        self.sysdef = cm_sysdef.Sysdef(sysdef=None, hookup_type=hookup_type)
        self.pols = self.sysdef.sysdef_json['signal_path_defs'][hookup_type]
        self.load_active(['parts', 'connections'])

    def update_workflow(self, node_csv='n'):
        self.load_gsheet(node_csv)
        self.make_sheet_connections()
        self.compare_connections()
        self.add_missing_parts()
        self.add_missing_connections()
        self.add_partial_connections()
        self.add_different_connections()
        self.finish()
        self.show_summary_of_compare()

    def make_sheet_connections(self):
        """
        Go through all of the sheet and make cm_tables.Connections for comparison.

        self.gsheet.connections is set up identically to self.active.connections
        """
        self.gsheet.connections = {'up': {}, 'down': {}}  # To mirror cm_active
        # Antenna Tab
        for ant in self.gsheet.ants:
            sheet_parts = {}
            # Set up part numbers
            for ptype in ['Station', 'Antenna']:
                sheet_parts[ptype] = self.sysdef.make_part_number(ant, ptype[0])
            sheet_parts['Feed'] = self.sysdef.make_part_number(self.gsheet.ants[ant][1], 'F')
            for this_part_prefix in ['PAX', 'RFCB', 'CBX', 'DBX', 'RBX']:
                val = self.gsheet.ants[ant][self.gsheet.header['Antenna'].index(this_part_prefix)]
                sheet_parts[this_part_prefix] = self.sysdef.make_part_number(val, this_part_prefix)
            # Set up connections
            for i, this_box in enumerate(['CBX', 'DBX', 'RBX']):
                if len(sheet_parts[this_box]):
                    up, dn = [sheet_parts["Antenna"], f"port{i}"], [sheet_parts[this_box], f"port"]
                    self._uconn(up, dn)
            up, dn = [sheet_parts["Station"], 'ground'], [sheet_parts["Antenna"], 'ground']
            self._uconn(up, dn)
            if len(sheet_parts["Feed"]):
                up, dn = [sheet_parts["Antenna"], 'focus'], [sheet_parts["Feed"], 'input']
            self._uconn(up, dn)
            for pol in self.pols:
                if len(sheet_parts["Feed"]) and len(sheet_parts['PAX']):
                    up, dn = [sheet_parts["Feed"], pol], [sheet_parts['PAX'], pol]
                    self._uconn(up, dn)
                if len(sheet_parts['PAX']) and len(sheet_parts['RFCB']):
                    up, dn = [sheet_parts['PAX'], pol], [sheet_parts['RFCB'], pol]
                    self._uconn(up, dn)
        # Tuning Tab - RFCB
        for rfcb, conns in self.gsheet.rfcbs.items():
            this_rfcb = self.sysdef.make_part_number(rfcb, 'RFCB')
            for conn in conns:
                this_attem = self.sysdef.make_part_number(int(conn[1].split(':')[0]), 'G')
                if len(this_attem):
                    rfcb_port = conn[0].split(':')[1]
                    attem_port = f"in{int(conn[1].split(':')[1])}"
                    up, dn = [this_rfcb, rfcb_port], [this_attem, attem_port]
                    self._uconn(up, dn)
        # Tuning Tab - RFSOC
        for rfsoc, conns in self.gsheet.rfsocs.items():
            this_rfsoc = self.sysdef.make_part_number(rfsoc, 'SOC')
            for conn in conns:
                this_attem = self.sysdef.make_part_number(int(conn[1].split(':')[0]), 'G')
                if len(this_attem):
                    attem_port = f"out{int(conn[1].split(':')[1])}"
                    rfsoc_port = f"in{int(conn[2].split(':')[1])}"
                    up, dn = [this_attem, attem_port], [this_rfsoc, rfsoc_port]
                    self._uconn(up, dn)
        # Tuning Tab - SNAP
        for snap, conns in self.gsheet.snaps.items():
            this_snap = self.sysdef.make_part_number(snap, 'SNAP')
            for conn in conns:
                this_attem = self.sysdef.make_part_number(int(conn[1].split(':')[0]), 'G')
                if len(this_attem):
                    attem_port = f"out{int(conn[1].split(':')[1])}"
                    snap_port = f"{conn[3].split(':')[1].lower()}"
                    up, dn = [this_attem, attem_port], [this_snap, snap_port]
                    self._uconn(up, dn)

    def _uconn(self, up, dn):
        self.gsheet.connections['up'].setdefault(up[0], {})
        self.gsheet.connections['up'][up[0]][up[1]] = cm_tables.Connections()
        self.gsheet.connections['up'][up[0]][up[1]].connection(
            upstream_part=up[0], upstream_output_port=up[1],
            downstream_part=dn[0], downstream_input_port=dn[1])
        self.gsheet.connections['down'].setdefault(dn[0], {})
        self.gsheet.connections['down'][dn[0]][dn[1]] = cm_tables.Connections()
        self.gsheet.connections['down'][dn[0]][dn[1]].connection(
            upstream_part=up[0], upstream_output_port=up[1],
            downstream_part=dn[0], downstream_input_port=dn[1])

    def compare_connections(self, direction='gsheet-active'):
        """
        Step through all of the sheet Connections and make sure they are all there and the same.
        """
        pside = 'up'
        self.compare_direction = direction
        self.missing = {}
        self.partial = {}
        self.diff_add = {}
        self.diff_stop = {}
        self.same = {}
        for pside in ['up', 'down']:
            self.missing[pside] = {}
            self.partial[pside] = {}
            self.diff_add[pside] = {}
            self.diff_stop[pside] = {}
            self.same[pside] = {}
            if direction.startswith('g'):
                A = self.gsheet.connections[pside]
                B = self.active.connections[pside]
            else:
                A = self.active.connections[pside]
                B = self.gsheet.connections[pside]
            for gkey, gpts in A.items():
                if gkey not in B.keys():
                    self.missing[pside][gkey] = copy(gpts)
                    continue
                for p, c in gpts.items():
                    if p not in B[gkey].keys():
                        self.partial[pside].setdefault(gkey, {})
                        self.partial[pside][gkey][p] = copy(c)
                    elif B[gkey][p] == c:
                        self.same[pside].setdefault(gkey, {})
                        self.same[pside][gkey][p] = copy(c)
                    else:
                        self.diff_add[pside].setdefault(gkey, {})
                        self.diff_add[pside][gkey][p] = copy(c)
                        self.diff_stop[pside].setdefault(gkey, {})
                        self.diff_stop[pside][gkey][p] = copy(B[gkey][p])

    def add_missing_parts(self):
        missing_parts = set()
        for pside in ['up', 'down']:
            for pc in self.missing[pside].values():
                for this_partconn in pc.values():
                    if this_partconn.upstream_part not in self.active.parts.keys():
                        missing_parts.add(this_partconn.upstream_part)
                    if this_partconn.downstream_part not in self.active.parts.keys():
                        missing_parts.add(this_partconn.downstream_part)
        self.missing_parts = list(missing_parts)
        if len(self.missing_parts):
            self.no_op_comment('Adding missing parts')
            add_part_time_offset = self.now - datetime.timedelta(seconds=300)
            cdate = add_part_time_offset.strftime('%Y/%m/%d')
            ctime = add_part_time_offset.strftime('%H:%M')
            for part in self.missing_parts:
                self.update_counter += 1
                this_part = [part, self.part_type.get_part_type(part), part]
                self.printit(upd_util.as_part('add', this_part, cdate, ctime))

    def add_missing_connections(self):
        if len(self.missing['up']) + len(self.missing['down']):
            self.no_op_comment('Adding missing connections')
            self._modify_connections(self.missing, 'add', self.cdate, self.ctime)

    def add_partial_connections(self):
        if len(self.partial['up']) + len(self.partial['down']):
            self.no_op_comment('Adding partial connections')
            self._modify_connections(self.partial, 'add', self.cdate, self.ctime)

    def add_different_connections(self):
        if len(self.diff_add['up']) + len(self.diff_add['down']) + len(self.diff_stop['up']) + len(self.diff_stop['down']):
            self.no_op_comment('Adding different connections')
            stop_conn_time_offset = self.now - datetime.timedelta(seconds=90)
            cdate = stop_conn_time_offset.strftime('%Y/%m/%d')
            ctime = stop_conn_time_offset.strftime('%H:%M')
            self._modify_connections(self.diff_stop, 'stop', cdate, ctime)
            self._modify_connections(self.diff_add, 'add', self.cdate, self.ctime)

    def _modify_connections(self, the_connections, add_or_stop, cdate, ctime):
        stop_or_add = 'add' if add_or_stop == 'stop' else 'stop'
        for pside in ['up', 'down']:
            this_one = the_connections[pside]
            for mod_conn in this_one.values():
                for conn in mod_conn.values():
                    cmpstr = f"{conn.upstream_part}{conn.upstream_output_port}{conn.downstream_part}{conn.downstream_input_port}"
                    if cmpstr in self.conn_track[add_or_stop]:
                        print(f"{cmpstr} already {add_or_stop}ed.")
                    elif cmpstr in self.conn_track[stop_or_add]:
                        print(f"!!!{cmpstr} in {add_or_stop} and {stop_or_add}!!!")
                    else:
                        self.conn_track[add_or_stop].append(cmpstr)
                        self.update_counter += 1
                        up = [conn.upstream_part, conn.upstream_output_port]
                        dn = [conn.downstream_part, conn.downstream_input_port]
                        self.printit(upd_util.as_connect(add_or_stop, up, dn, cdate, ctime))

    def show_summary_of_compare(self, check=False):
        from tabulate import tabulate
        header = ['Type', 'up', 'down', 'total']
        print("\n---Summary---")
        print(f"{self.update_counter} total number of updates.")
        print(f"{len(self.missing_parts)} parts added.")
        table_data = []
        for conn in ['same', 'missing', 'partial', 'diff_add', 'diff_stop']:
            up = len(getattr(self, conn)['up'])
            dn = len(getattr(self, conn)['down'])
            table_data.append([conn.capitalize(), up, dn, up + dn])
        print(tabulate(table_data, header))
        if check:
            print("PARTIAL")
            if len(self.partial):
                print("*****CHECK*****")
                for p in self.partial:
                    print("\t{}".format(p))
            else:
                print()
            print("DIFFERENT")
            if len(self.diff_add):
                print("*****CHECK*****")
                for d in self.diff_add:
                    print("\t{}".format(d))
            else:
                print()
            print("DIFFERENT_STOP")
            if len(self.diff_stop):
                print("*****CHECK*****")
                for d in self.diff_stop:
                    print("\t{}".format(d))
            else:
                print()
