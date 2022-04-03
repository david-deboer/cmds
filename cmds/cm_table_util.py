# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R. DeBoer
# Licensed under the 2-clause BSD license.

"""CM utils for the stations/parts and the connections between them."""

from . import cm, cm_utils, cm_tables
from sqlalchemy import func


no_connection_designator = "-X-"


def update_parts(parts, dates, session=None, override=False):
    """
    Add or stop parts.

    Parameters
    ----------
    parts : list of dicts
        List of dicts containing part information and action (add/stop)
    dates : list of astropy.Time objects
        List of dates to use for logging the add/stop.
    session : object
        Database session to use.  If None, it will start a new session, then close.
    override : bool
        Flag to allow override for existence or not.

    """
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = cm.connect_to_cm_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    updated = 0
    for partd, date in zip(parts, dates):
        pn = partd['pn'].upper()
        part = session.query(cm_tables.Parts).filter(func.upper(cm_tables.Parts.pn) == pn).first()
        if partd['action'].lower() == 'stop':
            if part is None:
                print("{} is not in database, so can't stop it.".format(pn))
                continue
            if part.stop_gpstime is not None:
                print("{} already has a stop time ({})".format(pn, part.stop_gpstime), end=' ')
                if not override:
                    print("==> Override not enabled.  No action.")
                    continue
                print("==> Override enabled.")
            print(f"Stopping part {pn} at {date}.")
            updated += part.part(stop_gpstime=date.gps)
            session.add(part)
        elif partd['action'].lower() == 'add':
            if part is None:
                part = cm_tables.Parts()
            else:
                print("{} already in database".format(pn), end=' ')
                if part.stop_gpstime is None:
                    print("with no stop date ==> no action.")
                    continue
                if not override:
                    print("and needs an override ==> no action.")
                    continue
                msg = f"Restarting part.  Previous data {part}"
                add_part_info(session, pn=pn, comment=msg, at_date=date.gps, reference="cm_table_util")
            print(f"Adding part {pn} for {date}")
            updated += part.part(pn=pn, ptype=partd['ptype'], manufacturer_id=partd['manufacturer_id'],
                                 start_gpstime=date.gps, stop_gpstime=None)
            session.add(part)
        else:
            print(f"Invalid option: {partd['action']}.")
    if updated:
        session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()
    return updated


def update_stations(session=None, data=None, add_new_station=False):
    """
    Update the stations table with some data.

    Use with caution -- should usually use in a script which will do datetime
    primary key etc.

    Parameters
    ----------
    session : session
        session on current database. If session is None, a new session
        on the default database is created and used.
    data : list of dicts
        dicts contain all Stations entries
    add_new_station : bool
        allow a new entry to be made.

    Returns
    -------
    int
        Number of entries changed

    """

    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = cm.connect_cm_db()
        session = db.sessionmaker()
        close_session_when_done = True

    made_change = 0
    for station in data:
        geo_rec = session.query(cm_tables.Stations).filter(
            func.upper(cm_tables.Stations.station_name) == station['station_name'].upper()
        )
        num_rec = geo_rec.count()
        if num_rec == 0:
            if add_new_station:
                gr = cm_tables.Stations()
            else:
                print("{} does not exist and add_new_station not enabled."
                      .format(station['station_name']))
                continue
        else:
            if add_new_station:
                print("{} exists and and_new_station is enabled.".format(station['station_name']))
                continue
            else:
                gr = geo_rec.first()
        if gr.station(**station):
            made_change += 1
            session.add(gr)
            # cm_utils.log("station update", data_dict=station)
    if made_change:
        session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()

    return made_change


def get_null_connection():
    """
    Return a null connection.

    All components hold the no_connection_designator and dates/times are None

    Returns
    -------
    Connections object

    """
    nc = no_connection_designator
    return cm_tables.Connections(
        upstream_part=nc,
        upstream_output_port=nc,
        downstream_part=nc,
        downstream_input_port=nc,
        start_gpstime=None,
        stop_gpstime=None,
    )


def update_connections(conns, dates, same_conn_sec=100, session=None, override=False):
    """
    Add or stop parts.

    Parameters
    ----------
    conns : list of dicts
        List of dicts containing connection information and action (add/stop)
    dates : list of astropy.Time objects
        List of dates to use for logging the add/stop.
    same_conn_sec : int
        Number of seconds under which the connection is the same.
    session : object
        Database session to use.  If None, it will start a new session, then close.
    override : bool
        Flag to allow override for existence or not.

    """
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = cm.connect_to_cm_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    updated = 0
    for connd, date in zip(conns, dates):
        connections_to_check = session.query(cm_tables.Connections).filter(
            (func.upper(cm_tables.Connections.upstream_part) == connd['upstream_part'].upper())
            & (func.lower(cm_tables.Connections.upstream_output_port)
               == connd['upstream_output_port'].lower())
            & (func.upper(cm_tables.Connections.downstream_part) == connd['downstream_part'].upper())
            & (func.lower(cm_tables.Connections.downstream_input_port)
               == connd['downstream_input_port'].lower())
        )
        if connd['action'].lower() == 'stop':
            if connections_to_check.count() == 0:
                print("Skipping: no connection listed for {}".format(connd))
                continue
            for connection in connections_to_check:
                if connection.stop_gpstime is None:
                    updated += connection.connection(stop_gpstime=date.gps)
                    print(f"Stopping {connection}")
                    session.add(connection)
                else:
                    print(f"Skipping: no unstopped connections for {connection}")
        elif connd['action'].lower() == 'add':
            if connections_to_check.count() == 0:
                connection = cm_tables.Connection()
                updated += connection.connection(
                    upstream_part=connd['upstream_part'],
                    upstream_output_port=connd['upstream_output_port'],
                    downstream_part=connd['downstream_part'],
                    downstream_input_port=connd['downstream_input_port'],
                    start_gpstime=date.gps, stop_gpstime=None)
                session.add(connection)
            for connection in connections_to_check:
                if abs(connection.start_gpstime - date.gps) < same_conn_sec:
                    print(f"{connection} is already started.", end=' ')
                    if connection.stop_gpstime is None:
                        print(" ==> Skipping.")
                    elif override:
                        print("with a stop time of {}".format(connection.stop_gpstime))
                        updated += connection.connection(
                            start_gpstime=date.gps, stop_gpstime=None)
                        session.add(connection)
    if updated:
        session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()
    return updated


def get_allowed_apriori_antenna_statuses():
    """Get list of valid apriori statuses."""
    apa = cm_tables.AprioriAntenna()
    return apa.valid_statuses()


def update_apriori_antenna(antenna, status, start_gpstime, stop_gpstime=None, session=None):
    """
    Update the 'apriori_antenna' status table to one of the class enum values.

    If the status is not allowed, an error will be raised.
    Adds the appropriate stop time to the previous apriori_antenna status.

    Parameters
    ----------
    antenna : str
        Antenna designator, e.g. HH104
    status : str
        Apriori status.  Must be one of apriori enums.
    start_gpstime : int
        Start time for new apriori status, in GPS seconds
    stop_gpstime : int
        Stop time for new apriori status, in GPS seconds, or None.
    session : object
        Database session to use.  If None, it will start a new session, then close.

    """
    new_apa = cm_tables.AprioriAntenna()

    if status not in new_apa.valid_statuses():
        raise ValueError(
            "Antenna apriori status must be in {}".format(new_apa.valid_statuses())
        )

    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = cm.connect_to_cm_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    antenna = antenna.upper()
    last_one = 1000
    old_apa = None
    for trial in session.query(cm_tables.AprioriAntenna).filter(
        func.upper(cm_tables.AprioriAntenna.antenna) == antenna
    ):
        if trial.start_gpstime > last_one:
            last_one = trial.start_gpstime
            old_apa = trial
    if old_apa is not None:
        if old_apa.stop_gpstime is None:
            old_apa.stop_gpstime = start_gpstime
        else:
            raise ValueError("Stop time must be None to update AprioriAntenna")
        session.add(old_apa)
    new_apa.antenna = antenna
    new_apa.status = status
    new_apa.start_gpstime = start_gpstime
    new_apa.stop_gpstime = stop_gpstime
    session.add(new_apa)

    session.commit()

    if close_session_when_done:  # pragma: no cover
        session.close()


def add_part_info(
    session, pn, comment, at_date, at_time=None, float_format=None, reference=None
):
    """
    Add part information into database.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    pn : str
        System part number
    at_date : any format that cm_utils.get_astropytime understands
        Date to use for the log entry
    at_time : any format that cm_utils.get_astropytime understands
        Time to use for the log entry, ignored if at_date is a float or contains time information
    float_format : str
        Format if at_date is unix or gps or jd day.
    comment : str
        String containing the comment to be logged.
    reference : str, None
        If appropriate, name or link of library file or other information.

    """
    comment = comment.strip()
    if not len(comment):
        import warnings

        warnings.warn("No action taken. Comment is empty.")
        return
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = cm.connect_to_cm_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    pi = cm_tables.PartInfo()
    pi.pn = pn
    pi.posting_gpstime = int(
        cm_utils.get_astropytime(at_date, at_time, float_format).gps
    )
    pi.comment = comment
    pi.reference = reference
    session.add(pi)
    session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()
