#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Find and display part hookups."""

import copy
from argparse import Namespace

from . import cm, cm_utils, cm_sysdef, cm_dossier, cm_active


def _polport(pol, port):
    if port == 'split':
        return pol.split('-')
    return f"{pol}-{port}"


class Hookup(object):
    """
    Class to find and display the signal path hookup information.

    Hookup traces parts and connections through the signal path (as defined
    by the connections in cm_sysdef).

    Parameters
    ----------
    session : session object or None
        If None, it will start a new session on the database.

    """

    def __init__(self, session=None):
        if session is None:  # pragma: no cover
            db = cm.connect_to_cm_db(None)
            self.session = db.sessionmaker()
        else:
            self.session = session
        self.active = None

    def get_hookup(self, pn, at_date='now', at_time=None, float_format=None,
                   exact_match=False, hookup_type=None):
        """
        Get the hookup dict from the database for the supplied match parameters.

        This gets called by the get_hookup wrapper if the database needs to be
        read (for instance, to generate a cache file, or search for parts
        different than those keyed on in the cache file.)

        Parameters
        ----------
        pn : str, list
            List/string of input hera part number(s) (whole or 'startswith')
            If string
                - 'default' uses default station prefixes in cm_sysdef
                - otherwise converts as csv-list
            If element of list is of format '.xxx:a/b/c' it finds the appropriate
                method as cm_sysdef.Sysdef.xxx([a, b, c])
        at_date : anything interpretable by cm_utils.get_astropytime
            Date at which to initialize.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to initialize, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day.
        exact_match : bool
            If False, will only check the first characters in each hpn entry.  E.g. 'HH1'
            would allow 'HH1', 'HH10', 'HH123', etc
        hookup_type : str or None
            Type of hookup to use (current observing system is 'parts_hera').
            If 'None' it will determine which system it thinks it is based on
            the part-type.  The order in which it checks is specified in cm_sysdef.
            Only change if you know you want a different system (like 'parts_paper').

        Returns
        -------
        dict
            Hookup dossier dictionary as defined in cm_dossier.HookupEntry

        """
        # Reset at_date
        at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        self.at_date = at_date
        self.active = cm_active.ActiveData(self.session, at_date=at_date)
        self.active.load_parts(at_date=None)
        self.active.load_connections(at_date=None)
        self.hookup_type = hookup_type
        self.sysdef = cm_sysdef.Sysdef(hookup_type)
        pn = cm_utils.listify(pn)
        parts_list = cm_utils.get_pn_list(pn, list(self.active.parts.keys()), exact_match)
        self.dossier = cm_dossier.Dossier(pn=parts_list, at_date=at_date, active=self.active,
                                          skip_pn_list_gather=True)
        hookup_dict = {}
        for this_part in parts_list:
            part = self.active.parts[this_part]
            hookup_dict[this_part] = cm_dossier.HookupEntry(entry_key=this_part, sysdef=self.sysdef)
            for pol in self.sysdef.polarizations:
                for port in self.dossier.dossier[this_part].input_ports:
                    if port in self.sysdef.components[part.ptype]['up'][pol]:
                        polport = _polport(pol, port)
                        hookup_dict[this_part].hookup[polport] = self._follow_hookup_stream(
                            part=this_part, pol=pol, port=port, dir="up")
                        hookup_dict[this_part].add_extras(polport, self.part_type_cache)
                for port in self.dossier.dossier[this_part].output_ports:
                    if port in self.sysdef.components[part.ptype]['down'][pol]:
                        polport = _polport(pol, port)
                        hookup_dict[this_part].hookup[polport] = self._follow_hookup_stream(
                            part=this_part, pol=pol, port=port, dir="down")
                        hookup_dict[this_part].add_extras(polport, self.part_type_cache)
        return hookup_dict

    def show_hookup(self, hookup_dict, cols_to_show="all", pols_to_show="all",
                    state="full", ports=False, sortby=None, filename=None, output_format="table"):
        """
        Generate a printable hookup table.

        Parameters
        ----------
        hookup_dict : dict
            Hookup dictionary generated in self.get_hookup
        cols_to_show : list, str
            list of columns to include in hookup listing
        state : str
            String designating whether to show the full hookups only, or all
        ports : bool
            Flag to include ports or not
        sortby : list, str or None
            Columns to sort the listed hookup.  None uses the keys.  str is a csv-list
            List items may have an argument separated by ':' for 'N'umber'P'refix'R'ev
            order (see cm_utils.put_keys_in_order).  Not included uses 'NRP'
        filename : str or None
            File name to use, None goes to stdout.  The file that gets written is
            in all cases an "ascii" file
        output_format : str
            Set output file type.
                'html' for a web-page version,
                'csv' for a comma-separated value version, or
                'table' for a formatted text table

        Returns
        -------
        str
            Table as a string

        """
        show = {"ports": ports}
        headers = self._make_header_row(hookup_dict, cols_to_show)
        table_data = []
        total_shown = 0
        sorted_hukeys = self._sort_hookup_display(hookup_dict, sortby, def_sort_order="NP")
        for hukey in sorted_hukeys:
            for ppk in cm_utils.put_keys_in_order(hookup_dict[hukey].hookup.keys(), sort_order="PN"):
                if not len(hookup_dict[hukey].hookup[ppk]):
                    continue
                use_this_row = False
                if state.lower() == "all":
                    use_this_row = True
                elif state.lower() == "full" and hookup_dict[hukey].fully_connected[ppk]:
                    use_this_row = True
                if not use_this_row:
                    continue
                total_shown += 1
                td = hookup_dict[hukey].table_entry_row(ppk, headers, show)
                if td not in table_data:
                    table_data.append(td)
        if total_shown == 0:
            print(
                "None found for {} (show-state is {})".format(
                    cm_utils.get_time_for_display(self.at_date), state
                )
            )
            return
        table = cm_utils.general_table_handler(headers, table_data, output_format)
        if filename is not None:
            with open(filename, "w") as fp:
                print(table, file=fp)
        return table

    # ##################################### Notes ############################################
    def get_notes(self, hookup_dict, state="all", return_dict=False):
        """
        Retrieve information for hookup.

        Parameters
        ----------
        hookup_dict : dict
            Hookup dictionary generated in self.get_hookup
        state : str
            String designating whether to show the full hookups only, or all
        return_dict : bool
            Flag to return a dictionary with additional information or just the note.

        Returns
        -------
        dict
            hookup notes

        """
        if self.active is None:
            self.active = cm_active.ActiveData(self.session, at_date=self.at_date)
        if self.active.info is None:
            self.active.load_info(self.at_date)
        info_keys = list(self.active.info.keys())
        hu_notes = {}
        for hkey in hookup_dict.keys():
            all_hu_hpn = set()
            for pol in hookup_dict[hkey].hookup.keys():
                for hpn in hookup_dict[hkey].hookup[pol]:
                    if state == "all" or (
                        state == "full" and hookup_dict[hkey].fully_connected[pol]
                    ):
                        all_hu_hpn.add(hpn.upstream_part)
                        all_hu_hpn.add(hpn.downstream_part)
            hu_notes[hkey] = {}
            for ikey in all_hu_hpn:
                if ikey in info_keys:
                    hu_notes[hkey][ikey] = {}
                    for entry in self.active.info[ikey]:
                        if return_dict:
                            hu_notes[hkey][ikey][entry.posting_gpstime] = {
                                "note": entry.comment.replace("\\n", "\n"),
                                "ref": entry.reference,
                            }
                        else:
                            hu_notes[hkey][ikey][
                                entry.posting_gpstime
                            ] = entry.comment.replace("\\n", "\n")
        return hu_notes

    def show_notes(self, hookup_dict, state="all"):
        """
        Print out the information for hookup.

        Parameters
        ----------
        hookup_dict : dict
            Hookup dictionary generated in self.get_hookup
        state : str
            String designating whether to show the full hookups only, or all

        Returns
        -------
        str
            Content as a string

        """
        hu_notes = self.get_notes(
            hookup_dict=hookup_dict, state=state, return_dict=True
        )
        full_info_string = ""
        for hkey in cm_utils.put_keys_in_order(list(hu_notes.keys()), sort_order="NPR"):
            hdr = "---{}---".format(hkey)
            entry_info = ""
            part_hu_hpn = cm_utils.put_keys_in_order(
                list(hu_notes[hkey].keys()), sort_order="PNR"
            )
            if hkey in part_hu_hpn:  # Do the hkey first
                part_hu_hpn.remove(hkey)
                part_hu_hpn = [hkey] + part_hu_hpn
            for ikey in part_hu_hpn:
                gps_times = sorted(hu_notes[hkey][ikey].keys())
                for gtime in gps_times:
                    atime = cm_utils.get_time_for_display(gtime, float_format="gps")
                    this_note = "{} ({})".format(
                        hu_notes[hkey][ikey][gtime]["note"],
                        hu_notes[hkey][ikey][gtime]["ref"],
                    )
                    entry_info += "\t{} ({})  {}\n".format(ikey, atime, this_note)
            if len(entry_info):
                full_info_string += "{}\n{}\n".format(hdr, entry_info)
        return full_info_string

    # ################################ Internal methods ######################################
    def _follow_hookup_stream(self, part, pol, port, dir):
        """
        Follow a list of connections upstream and downstream.

        Parameters
        ----------
        part : str
            HERA part number
        pol : str
            Polarization designation
        port : str
            Port designation
        dir : str
            Direction to follow (up or down)

        Returns
        -------
        list
            List of connections for that hookup.

        """
        stream = {'up': [], 'down': []}
        starting_part = part.upper()
        starting_part_type = self.active.parts[part].ptype
        self.part_type_cache = {starting_part: starting_part_type}
        current = Namespace(
            direction=dir,
            part=starting_part,
            pol=pol,
            type=starting_part_type,
            port=port.lower(),
            stream=stream[dir]
        )
        self._recursive_connect(current)
        port = self.sysdef.get_thru_port(port, dir, pol, starting_part_type)
        dir = cm_sysdef.opposite_direction[dir]
        current = Namespace(
            direction=dir,
            part=starting_part,
            pol=pol,
            type=starting_part_type,
            port=port.lower(),
            stream=stream[dir]
        )
        self._recursive_connect(current)
        hu = []
        for pn in reversed(stream['up']):
            hu.append(pn)
        for pn in stream['down']:
            hu.append(pn)
        return hu

    def _recursive_connect(self, current):
        """
        Find the next connection up the signal chain.

        Parameters
        ----------
        current : Namespace object
            Namespace containing current information.

        """
        conn, current = self._get_connection(current)  # rewrites current
        if conn is None:
            return None
        current.stream.append(conn)
        self._recursive_connect(current)

    def _get_connection(self, current):
        """
        Get next connected part going the given direction.

        Parameters
        ----------
        current : Namespace object
            Namespace containing current information, which gets updated.

        """
        # Get the next port we want.
        oside = cm_sysdef.opposite_direction[current.direction]
        if current.port is None:
            return None, None
        try:
            this_conn = self.active.connections[oside][current.part][current.port]
        except KeyError:
            return None, None
        # Now increment the connection up/down the chain
        if current.direction == "up":
            current.part = this_conn.upstream_part.upper()
            port1 = this_conn.upstream_output_port.lower()
        elif current.direction == "down":
            current.part = this_conn.downstream_part.upper()
            port1 = this_conn.downstream_input_port.lower()
        current.type = self.active.parts[current.part].ptype
        self.part_type_cache[current.part] = current.type
        try:
            options = list(self.active.connections[oside][current.part].keys())
        except KeyError:
            options = None
        if options is None:
            current.port = None
        else:
            next_port = self.sysdef.get_thru_port(port1, oside, current.pol, current.type)
            if next_port in options:
                current.port = next_port
            else:
                current.port = None
        return this_conn, current

    def _sort_hookup_display(self, hookup_dict, sortby=None, def_sort_order="NP"):
        if sortby is None:
            return cm_utils.put_keys_in_order(hookup_dict.keys(), sort_order="NP")
        print("ONLY SORTBY NONE IS OK")
        return None
        if isinstance(sortby, str):
            sortby = sortby.split(",")
        sort_order_dict = {}
        for stmp in sortby:
            ss = stmp.split(":")
            if ss[0] in self.col_list:
                if len(ss) == 1:
                    ss.append(def_sort_order)
                sort_order_dict[ss[0]] = ss[1]
        if "station" not in sort_order_dict.keys():
            sortby.append("station")
            sort_order_dict["station"] = "NP"
        key_bucket = {}
        show = {"ports": False}
        for this_key, this_hu in hookup_dict.items():
            pk = list(this_hu.hookup.keys())[0]
            this_entry = this_hu.table_entry_row(pk, sortby, self.part_type_cache, show)
            ekey = []
            for eee in [
                cm_utils.peel_key(x, sort_order_dict[sortby[i]])
                for i, x in enumerate(this_entry)
            ]:
                ekey += eee
            key_bucket[tuple(ekey)] = this_key
        sorted_keys = []
        for _k, _v in sorted(key_bucket.items()):
            sorted_keys.append(_v)
        return sorted_keys

    def _make_header_row(self, hookup_dict, cols_to_show):
        """
        Generate the appropriate header row for the displayed hookup.

        Parameters
        ----------
        hookup_dict : dict
            Hookup dictionary generated in self.get_hookup
        cols_to_show : list
            list of columns to include in hookup listing

        Returns
        -------
        list
            List of header titles.

        """
        if cols_to_show[0] == 'all':
            return self.sysdef.hookup + ['start', 'stop']
        self.col_list = []
        for h in hookup_dict.values():
            for cols in h.columns.values():
                if len(cols) > len(self.col_list):
                    self.col_list = copy.copy(cols)
        if isinstance(cols_to_show, str):
            cols_to_show = cols_to_show.split(",")
        cols_to_show = [x.lower() for x in cols_to_show]
        if "all" in cols_to_show:
            return self.col_list
        headers = []
        for col in self.col_list:
            if col.lower() in cols_to_show:
                headers.append(col)
        return headers
