#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Holds helpful modules for parts and connections scripts."""

import copy
from astropy.time import Time
from sqlalchemy import func

from . import cm, cm_utils, cm_dossier
from . import cm_partconnect as partconn


class Handling:
    """
    Class to allow various manipulations of parts, connections and their properties etc.

    Parameters
    ----------
    session : object
        session on current database. If session is None, a new session
        on the default database is created and used.

    """

    def __init__(self, session=None):
        if session is None:  # pragma: no cover
            db = cm.connect_to_cm_db(None)
            self.session = db.sessionmaker()
        else:
            self.session = session

    def close(self):  # pragma: no cover
        """Close the session."""
        self.session.close()

    def get_part_type_for(self, pn):
        """
        Provide the signal path part type for a supplied part number.

        Parameters
        ----------
        pn : str
            system part number.

        Returns
        -------
        str
            The associated part type.

        """
        part_query = (
            self.session.query(partconn.Parts)
            .filter((func.upper(partconn.Parts.pn) == pn.upper()))
            .first()
        )
        return part_query.hptype

    def get_part_from_pn(self, pn):
        """
        Return a Part object for the supplied part number.

        Parameters
        ----------
        pn : str
            HERA part number

        Returns
        -------
        Part object

        """
        return (
            self.session.query(partconn.Parts)
            .filter(
                (func.upper(partconn.Parts.hpn) == pn.upper())
            )
            .first()
        )

    def _get_allowed_ports(self, ports):
        """
        Get the allowed_ports class variable for requested ports.

        Parameters
        ----------
        ports : list/str/None
            Desired ports per show_dossier

        """
        self.allowed_ports = None
        if isinstance(ports, str):
            ports = ports.split(",")
        if isinstance(ports, list):
            self.allowed_ports = [x.upper() for x in ports]

    def get_dossier(
        self,
        pn,
        at_date="now",
        at_time=None,
        float_format=None,
        active=None,
        notes_start_date="<",
        notes_start_time=None,
        notes_float_format=None,
        exact_match=True,
    ):
        """
        Get information on a part or parts.

        Parameters
        ----------
        pn : str, list
            Part number [string or list-of-strings] (whole or first part thereof)
        at_date : anything interpretable by cm_utils.get_astropytime
            Date for which to check.
        at_time : anything interpretable by cm_utils.get_astropytime
            Time at which to check, ignored if at_date is a float or contains time information
        float_format : str
            Format if at_date is a number denoting gps or unix seconds or jd day
        active : cm_active.ActiveData class or None
            Use supplied ActiveData.  If None, read in.
        notes_start_date : anything interpretable by cm_utils.get_astropytime
            Start_date for displaying notes
        notes_start_time : anything interpretable by cm_utils.get_astropytime
            Start time for displaying notes, ignored if notes_start_date is a float or
            contains time information
        notes_float_format : str
            Format if notes_start_date is a number denoting gps or unix seconds or jd day.
        exact_match : bool
            Flag to enforce full part number match, or "startswith"

        Returns
        -------
        dict
            dictionary keyed on the part_number:rev containing PartEntry
            dossier classes

        """
        from . import cm_active

        at_date = cm_utils.get_astropytime(at_date, at_time, float_format)
        notes_start_date = cm_utils.get_astropytime(
            notes_start_date, notes_start_time, notes_float_format
        )
        if active is None:
            active = cm_active.ActiveData(self.session, at_date=at_date)
        elif at_date is not None:
            date_diff = abs(at_date - active.at_date).sec
            if date_diff > 1.0:
                raise ValueError(
                    "Supplied date and active date do not agree "
                    "({}sec)".format(date_diff)
                )
        else:
            at_date = active.at_date
        if active.parts is None:
            active.load_parts(at_date=at_date)
        if active.connections is None:
            active.load_connections(at_date=at_date)
        if active.info is None:
            active.load_info(at_date=at_date)
        if active.geo is None:
            active.load_geo(at_date=at_date)
        part_dossier = {}

        pn_list = self._get_pn_list(pn, active, exact_match)

        for loop_pn in pn_list:
            if loop_pn in active.parts.keys():
                this_part = cm_dossier.PartEntry(
                    pn=loop_pn,
                    at_date=at_date,
                    notes_start_date=notes_start_date,
                )
                this_part.get_entry(active)
                part_dossier[loop_pn] = this_part

        return part_dossier

    def show_dossier(self, dossier, columns, ports=None):
        """
        Generate part information print string.  Uses tabulate package.

        Parameter
        ---------
        dossier : dict
            Input dictionary of parts, generated by self.get_dossier
        columns : list
            List of column headers to use.
        ports : list, str, None
            Ports to show.
            If None, counterintuitively, all are included
                (see cm_sysdef.all_port_types)
            If str, it assumes that types are provided
                (see cm_sysdef.all_port_types), specified as csv-list.
            If list, it only allows those.

        Returns
        -------
        str
            String containing the dossier table.
        """
        from tabulate import tabulate

        self._get_allowed_ports(ports)
        pd_keys = cm_utils.put_keys_in_order(list(dossier.keys()))
        if len(pd_keys) == 0:
            return "Part not found"
        table_data = []
        headers = dossier[pd_keys[0]].get_headers(columns=columns)
        for hpnr in pd_keys:
            new_rows = dossier[hpnr].table_row(
                columns=columns, ports=self.allowed_ports
            )
            for nr in new_rows:
                table_data.append(nr)
        return "\n" + tabulate(table_data, headers=headers, tablefmt="orgtbl") + "\n"

    def get_specific_connection(self, cobj, at_date=None):
        """
        Find a list of connections matching the supplied components of the query.

        At the very least upstream_part and downstream_part must be included
        -- revisions and ports are ignored unless they are of type string.
        If at_date is of type Time, it will only return connections valid at
        that time.  Otherwise it ignores at_date (i.e. it will return any such
        connection over all time.)

        Parameters
        ----------
        cobj : object
            Connection class containing the query
        at_date : Astropy Time
            Time to check epoch.  If None is ignored.

        Returns
        -------
        list
            List of Connections

        """
        fnd = []
        for conn in self.session.query(partconn.Connections).filter(
            (
                func.upper(partconn.Connections.upstream_part)
                == cobj.upstream_part.upper()
            )
            & (
                func.upper(partconn.Connections.downstream_part)
                == cobj.downstream_part.upper()
            )
        ):
            conn.gps2Time()
            include_this_one = True
            if (
                isinstance(cobj.up_part_rev, str)
                and cobj.up_part_rev.lower() != conn.up_part_rev.lower()
            ):
                include_this_one = False
            if (
                isinstance(cobj.down_part_rev, str)
                and cobj.down_part_rev.lower() != conn.down_part_rev.lower()
            ):
                include_this_one = False
            if (
                isinstance(cobj.upstream_output_port, str)
                and cobj.upstream_output_port.lower()
                != conn.upstream_output_port.lower()
            ):
                include_this_one = False
            if (
                isinstance(cobj.downstream_input_port, str)
                and cobj.downstream_input_port.lower()
                != conn.downstream_input_port.lower()
            ):
                include_this_one = False
            if isinstance(at_date, Time) and not cm_utils.is_active(
                at_date, conn.start_date, conn.stop_date
            ):
                include_this_one = False
            if include_this_one:
                fnd.append(copy.copy(conn))
        return fnd
