# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R. DeBoer
# Licensed under the 2-clause BSD license.

"""CM utils for the stations/parts and the connections between them."""

from . import cm, cm_tables
from sqlalchemy import func


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
