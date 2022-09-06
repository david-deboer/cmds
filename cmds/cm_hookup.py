#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Find and display part hookups."""

from argparse import Namespace
from copy import copy

from . import cm_utils, cm_sysdef, cm_dossier, cm_active


def _polport(pol, port, demarc='-'):
    if port == 'split':
        return pol.split(demarc)
    return f"{pol}{demarc}{port}"


def get_hookup(
    pn,
    pol="all",
    at_date="now",
    at_time=None,
    float_format=None,
    exact_match=False,
    hookup_type=None,
    show=True,
):
    """
    Return a hookup object.

    This allows for a simple way to get a part hookup, however if more
    transactions are needed, please use the the session wrapper in cm to
    generate a context managed session and pass that session to the class.

    Parameters
    ----------
    pn : str, list
        List/string of input part number(s) (whole or 'startswith')
        If string
            - 'default' uses default station prefixes in cm_sysdef
            - otherwise converts as csv-list
        If element of list is of format '.xxx:a/b/c' it finds the appropriate
            method as cm_sysdef.Sysdef.xxx([a, b, c])
    pol : str
        A port polarization to follow, or 'all',  ('e', 'n', 'all')
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
        Type of hookup to use (current observing system is 'parts_').
        If 'None' it will determine which system it thinks it is based on
        the part-type.  The order in which it checks is specified in cm_sysdef.
        Only change if you know you want a different system (like 'parts_paper').

    Returns
    -------
    Hookup object
        Hookup dossier object

    """
    from . import cm

    with cm.CMSessionWrapper() as session:
        hookup = Hookup(session=session)
        hookup.get_hookup(
            pn=pn,
            pol=pol,
            at_date=at_date,
            at_time=at_time,
            float_format=float_format,
            exact_match=exact_match,
            hookup_type=hookup_type,
        )
        if show:
            print(hookup.show_hookup())
        return hookup


class Hookup(object):
    """
    Class to find and display the signal path hookup information.

    Hookup traces parts and connections through the signal path (as defined
    by the connections in cm_sysdef).

    Parameters
    ----------
    session : session object
        Session from sessionmaker

    """

    def __init__(self, session):
        self.session = session
        self.active = None

    def get_hookup(self, pn, pol='all', at_date='now', at_time=None, float_format=None,
                   exact_match=False, hookup_type=None, active=None):
        """
        Get the hookup dict from the database for the supplied match parameters.

        Parameters
        ----------
        pn : str, list
            List/string of input part number(s) (whole or 'startswith')
            If string
                - 'default' uses default station prefixes in cm_sysdef
                - otherwise converts as csv-list
            If element of list is of format '.xxx:a/b/c' it finds the appropriate
                method as cm_sysdef.Sysdef.xxx([a, b, c])
        pol : str
            A port polarization to follow, or 'all',  ('e', 'n', 'all')
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
        active : None or ActiveData object
            If None, read in new.

        Returns
        -------
        dict
            Hookup dossier dictionary as defined in cm_hookup_entry.HookupEntry keyed on part.

        """
        self.sysdef = cm_sysdef.Sysdef(hookup_type)

        if isinstance(pol, str):
            if pol == 'all':
                pol = self.sysdef.polarizations
            else:
                pol = pol.split(',')
        self.at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        if active is None:
            self.active = cm_active.ActiveData(self.session, at_date=self.at_date)
        else:
            self.active = active
        self.active.load_parts(at_date=None)
        self.active.load_connections(at_date=None)
        pn = cm_utils.listify(pn)
        parts_list = cm_utils.get_pn_list(pn, list(self.active.parts.keys()), exact_match)
        self.dossier = cm_dossier.Dossier(pn=parts_list, at_date=self.at_date, active=self.active,
                                          skip_pn_list_gather=True)
        self.hookup = {}
        for this_part in parts_list:
            part = self.active.parts[this_part]
            self.hookup[this_part] = HookupEntry(entry_key=this_part, sysdef=self.sysdef)
            for this_pol in pol:
                for this_port in self.dossier.dossier[this_part].input_ports:
                    if this_port in self.sysdef.components[part.ptype]['up'][this_pol]:
                        print('CMH169:  up', this_part, this_pol, this_port)
                        polport = _polport(this_pol, this_port)
                        self.hookup[this_part].hookup[polport] = self._follow_hookup_stream(
                            part=this_part, pol=this_pol, port=this_port, dir="up")
                        self.hookup[this_part].add_extras(polport, self.part_type_cache)
                for this_port in self.dossier.dossier[this_part].output_ports:
                    if this_port in self.sysdef.components[part.ptype]['down'][this_pol]:
                        print('CMH176:  down', this_part, this_pol, this_port)
                        polport = _polport(this_pol, this_port)
                        self.hookup[this_part].hookup[polport] = self._follow_hookup_stream(
                            part=this_part, pol=this_pol, port=this_port, dir="down")
                        self.hookup[this_part].add_extras(polport, self.part_type_cache)

    def _make_header_row(self, cols_to_show, timing):
        """
        Generate the appropriate header row for the hookup object.

        Parameters
        ----------
        cols_to_show : list
            list of columns to include in hookup listing
        timing : bool
            flag to include timing

        Returns
        -------
        list
            List of header titles.

        """
        if isinstance(cols_to_show, str):
            cols_to_show = cols_to_show.split(',')
        if cols_to_show[0] == 'all':
            headers = self.sysdef.hookup + []
        else:
            self.col_list = []
            for h in self.hookup.values():
                for col in h.columns:
                    if col not in self.col_list:
                        self.col_list.append(col.lower())
            headers = []
            for col in cols_to_show:
                if col.lower() in self.col_list:
                    headers.append(col)
        if timing:
            headers += ['start', 'stop']
        return headers

    def show_hookup(self, cols_to_show="all", state="full",
                    pols_to_show="all", ports=False, sortby=None, timing=True,
                    filename=None, output_format="table"):
        """
        Generate a printable hookup table.

        Parameters
        ----------
        cols_to_show : list, str
            list of columns to include in hookup listing
        state : str
            String designating whether to show the full hookups only, or all
        pols_to_show : list, str
            List of polarizations or 'all'
        ports : bool
            Flag to include ports or not
        sortby : list, str or None
            Columns to sort the listed hookup.  None uses the keys.  str is a csv-list
            List items may have an argument separated by ':' for 'N'umber'P'refix'R'ev
            order (see cm_utils.put_keys_in_order).  Not included uses 'NRP'
        timing : bool
            Include start/stop in table.
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
        headers = self._make_header_row(cols_to_show, timing=timing)
        table_data = []
        total_shown = 0
        sorted_hukeys = self._sort_hookup_display(sortby, def_sort_order="NP")
        for hukey in sorted_hukeys:
            for ppk in cm_utils.put_keys_in_order(self.hookup[hukey].hookup.keys(), sort_order="PN"):
                if self.hookup[hukey].hookup[ppk] is not None and len(self.hookup[hukey].hookup[ppk]):
                    this_state, is_full = state.lower(), self.hookup[hukey].fully_connected[ppk]
                    if this_state == "all" or (this_state == "full" and is_full):
                        total_shown += 1
                        td = self.hookup[hukey].table_entry_row(ppk, headers, show)
                        if td not in table_data:
                            table_data.append(td)
        if total_shown == 0:
            print("None found for {} (show-state is {})".format(
                cm_utils.get_time_for_display(self.at_date), state))
            return
        table = cm_utils.general_table_handler(headers, table_data, output_format)
        if filename is not None:
            with open(filename, "w") as fp:
                print(table, file=fp)
        return table

    # ##################################### Notes ############################################
    def get_notes(self, state="all", return_dict=False):
        """
        Retrieve information for hookup.

        Parameters
        ----------
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
        self.notes = {}
        for hkey in self.hookup.keys():
            all_hu_hpn = set()
            for pol in self.hookup[hkey].hookup.keys():
                for hpn in self.hookup[hkey].hookup[pol]:
                    if state == "all" or (
                        state == "full" and self.hookup[hkey].fully_connected[pol]
                    ):
                        all_hu_hpn.add(hpn.upstream_part)
                        all_hu_hpn.add(hpn.downstream_part)
            self.notes[hkey] = {}
            for ikey in all_hu_hpn:
                if ikey in info_keys:
                    self.notes[hkey][ikey] = {}
                    for entry in self.active.info[ikey]:
                        if return_dict:
                            self.notes[hkey][ikey][entry.posting_gpstime] = {
                                "note": entry.comment.replace("\\n", "\n"),
                                "ref": entry.reference,
                            }
                        else:
                            self.notes[hkey][ikey][
                                entry.posting_gpstime
                            ] = entry.comment.replace("\\n", "\n")

    def show_notes(self, state="all"):
        """
        Print out the information for hookup.

        Parameters
        ----------
        state : str
            String designating whether to show the full hookups only, or all

        Returns
        -------
        str
            Content as a string

        """
        self.get_notes(state=state, return_dict=True)
        full_info_string = ""
        for hkey in cm_utils.put_keys_in_order(list(self.notes.keys()), sort_order="NPR"):
            hdr = "---{}---".format(hkey)
            entry_info = ""
            part_hu_hpn = cm_utils.put_keys_in_order(
                list(self.notes[hkey].keys()), sort_order="PNR"
            )
            if hkey in part_hu_hpn:  # Do the hkey first
                part_hu_hpn.remove(hkey)
                part_hu_hpn = [hkey] + part_hu_hpn
            for ikey in part_hu_hpn:
                gps_times = sorted(self.notes[hkey][ikey].keys())
                for gtime in gps_times:
                    atime = cm_utils.get_time_for_display(gtime, float_format="gps")
                    this_note = "{} ({})".format(
                        self.notes[hkey][ikey][gtime]["note"],
                        self.notes[hkey][ikey][gtime]["ref"],
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
        try:
            plower = port.lower()
        except AttributeError:
            return
        current = Namespace(
            direction=dir,
            part=starting_part,
            pol=pol,
            type=starting_part_type,
            port=plower,
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
        if current.direction == 'down':
            print(f"CMH433-----{current.direction} {current.port}: {conn.upstream_part} --> {conn.downstream_part}")
        else:
            print(f"CMH436-----{current.direction} {current.port}: {conn.downstream_part} --> {conn.upstream_part}")
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

    def _sort_hookup_display(self, sortby=None, def_sort_order="NP"):
        if sortby is None:
            return cm_utils.put_keys_in_order(self.hookup.keys(), sort_order="NP")
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
        for this_key, this_hu in self.hookup.items():
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


class HookupEntry(object):
    """
    Holds the structure of the hookup entry.  All are keyed on polarization-port.

    Parameters
    ----------
    entry_key : str
        Entry key to use for the entry.  Must be None if input_dict is not None.
    input_dict : dict
        Dictionary with seed hookup.  If it is None, entry_key and sysdef must both be provided.

    """

    def __init__(self, entry_key=None, sysdef=None):
        self.hookup = {}  # actual hookup connection information
        self.fully_connected = {}  # flag if fully connected
        self.columns = {}  # list with the actual column headers in hookup
        self.timing = {}  # aggregate hookup start and stop
        self.part_type = {}
        self.sysdef = sysdef
        self.columns = copy(self.sysdef.hookup)
        self.columns.append("start")
        self.columns.append("stop")

    def __repr__(self):
        """Define representation."""
        s = str(self.hookup)
        return s

    def add_extras(self, polport, pt_cache):
        """
        Add the timing and fully_connected flag for the hookup.

        Parameters
        ----------
        polport : str
            Part port to get.

        """
        if polport is None or self.hookup[polport] is None:
            return
        full_hookup_length = len(self.sysdef.hookup) - 1
        latest_start = 0
        earliest_stop = None
        self.part_type[polport] = []
        for c in self.hookup[polport]:
            self.part_type[polport].append(pt_cache[c.upstream_part])
            if c.start_gpstime > latest_start:
                latest_start = c.start_gpstime
            if c.stop_gpstime is None:
                pass
            elif earliest_stop is None:
                earliest_stop = c.stop_gpstime
            elif c.stop_gpstime < earliest_stop:
                earliest_stop = c.stop_gpstime
        self.part_type[polport].append(pt_cache[self.hookup[polport][-1].downstream_part])
        self.timing[polport] = [latest_start, earliest_stop]
        self.fully_connected[polport] = len(self.hookup[polport]) == full_hookup_length

    def table_entry_row(self, polport, cols, show):
        """
        Produce the hookup table row for given parameters.

        Parameters
        ----------
        polport : str
            Polarization type (specified in 'cm_sysdef')
        cols : list
            Columns to include
        show : dict
            Dictionary containing flags of what components to show.

        Returns
        -------
        list
            List containing the table entry.

        """
        timing = self.timing[polport]
        td = ["-"] * len(cols)

        # Get the first N-1 parts
        dip = ""
        for part_type, c in zip(self.part_type[polport], self.hookup[polport]):
            if part_type in cols:
                new_row_entry = self._build_new_row_entry(
                    dip, c.upstream_part, c.upstream_output_port, show
                )
                td[cols.index(part_type)] = new_row_entry
            dip = c.downstream_input_port + "> "
        # Get the last part in the hookup
        part_type = self.part_type[polport][-1]
        if part_type in cols:
            new_row_entry = self._build_new_row_entry(
                dip, c.downstream_part, None, show
            )
            td[cols.index(part_type)] = new_row_entry
        # Add timing
        if "start" in cols:
            td[cols.index("start")] = timing[0]
        if "stop" in cols:
            td[cols.index("stop")] = timing[1]
        return td

    def _build_new_row_entry(self, dip, part, port, show):
        """
        Format the hookup row entry.

        Parameters
        ----------
        dip : str
            Current entry display for the downstream_input_port
        part : str
            Current part name
        port : str
            Current port name
        show : dict
            Dictionary containing flags of what components to show.

        Returns
        -------
        str
            String containing that row entry.

        """
        new_row_entry = ""
        if show["ports"]:
            new_row_entry = dip
        new_row_entry += part
        if port is not None and show["ports"]:
            new_row_entry += " <" + port
        return new_row_entry
