#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Holds helpful modules for parts and connections scripts."""

import copy
from astropy.time import Time
from sqlalchemy import func

from . import cm, cm_utils, cm_tables


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
            self.session.query(cm_tables.Parts)
            .filter((func.upper(cm_tables.Parts.pn) == pn.upper()))
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
            self.session.query(cm_tables.Parts)
            .filter(
                (func.upper(cm_tables.Parts.hpn) == pn.upper())
            )
            .first()
        )

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
        for conn in self.session.query(cm_tables.Connections).filter(
            (
                func.upper(cm_tables.Connections.upstream_part)
                == cobj.upstream_part.upper()
            )
            & (
                func.upper(cm_tables.Connections.downstream_part)
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
