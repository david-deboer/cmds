# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""M&C logging of the parts and the connections between them."""

from astropy.time import Time
from sqlalchemy import BigInteger, Column, ForeignKeyConstraint, String, Text, func
from . import MCDeclarativeBase, NotNull
from . import mc, cm_utils

no_connection_designator = "-X-"


class Parts(MCDeclarativeBase):
    """
    A table logging parts within the system.

    Stations will be considered parts of type='station'
    Note that ideally install_date would also be a primary key, but that
    screws up ForeignKey in connections

    Attributes
    ----------
    pn : String Column
        System part number for each part; part of the primary key.
    ptype : String Column
        A part-dependent string, i.e. feed, frontend, ...
    manufacturer_number : String Column
        A part number/serial number as specified by manufacturer
    start_gpstime : BigInteger Column
        The date when the part was installed (or otherwise assigned by project).
    stop_gpstime : BigInteger Column
        The date when the part was removed (or otherwise de-assigned by project).

    """

    __tablename__ = "parts"

    pn = Column(String(64), primary_key=True)
    ptype = NotNull(String(64))
    manufacturer_number = Column(String(64))
    start_gpstime = Column(BigInteger, nullable=False)
    stop_gpstime = Column(BigInteger)

    def __repr__(self):
        """Define representation."""
        return (
            "<sysPartNumber id={self.pn} "
            "type={self.ptype} :: "
            "{self.start_gpstime} - {self.stop_gpstime}>".format(
                self=self
            )
        )

    def __eq__(self, other):
        """Define equality."""
        if (
            isinstance(other, self.__class__)
            and self.pn.upper() == other.pn.upper()
        ):
            return True
        return False

    def gps2Time(self):
        """Make astropy.Time object from gps."""
        self.start_date = Time(self.start_gpstime, format="gps")
        if self.stop_gpstime is None:
            self.stop_date = None
        else:
            self.stop_date = Time(self.stop_gpstime, format="gps")

    def part(self, **kwargs):
        """Allow specification of an arbitrary part."""
        for key, value in kwargs.items():
            setattr(self, key, value)


def stop_existing_parts(session, parts, stop_dates, allow_override=False):
    """
    Add stop times to the previous parts.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    parts : list of str
        List containing part numbers.
    stop_dates : list of astropy.Time objects
        List of dates to use for logging the stop
    allow_override : bool
        Flag to allow a reset of the stop time even if one exists.

    """
    data = []
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    for pn, stop_date in zip(parts, stop_dates):
        existing = (
            session.query(Parts)
            .filter(
                (func.upper(Parts.pn) == pn.upper())
            )
            .first()
        )
        if existing is None:
            print("{} is not found, so can't stop it.".format(pn))
            continue
        if existing.stop_gpstime is not None:
            print(
                "{} already has a stop time ({})".format(
                    pn, existing.stop_gpstime
                )
            )
            if allow_override:
                print("\tOverride enabled.   New value {}".format(stop_date))
            else:
                print("\tOverride not enabled.  No action.")
                continue
        else:
            print("Stopping part {} at {}".format(pn, str(stop_date)))
        data.append([pn, "stop_gpstime", stop_date.gps])

    _update_part(session, data)
    if close_session_when_done:  # pragma: no cover
        session.close()


def add_new_parts(session, parts, start_dates, allow_restart=False):
    """
    Add new parts.

    If a part is there and is stopped, it will log that info
    and restart the part.  If it is there and is not stopped, it does nothing.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    parts : list of lists
        List containing [[part number, part type, manufacturer number], ...].
    start_dates : list of astropy.Time objects
        List of dates to use for logging the addition.
    allow_restart : bool
        Flag to allow the part to restarted if it already existed.

    """
    data = []
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    for pdes, start_date in zip(parts, start_dates):
        pn, ptype, pman = pdes
        existing = (
            session.query(Parts)
            .filter(
                (func.upper(Parts.pn) == pn.upper())
            )
            .first()
        )
        if existing is not None and existing.stop_gpstime is None:
            print(
                "No action. {} already in database with no stop date".format(
                    pn
                )
            )
            continue
        this_data = []
        this_data.append([pn, "pn", pn])
        this_data.append([pn, "ptype", ptype])
        this_data.append([pn, "manufacturer_number", pman])
        this_data.append([pn, "start_gpstime", start_date.gps])
        print_out = "starting part {} at {}".format(pn, str(start_date))
        if existing is not None:
            if allow_restart:
                print_out = "re" + print_out
                this_data.append([pn, "stop_gpstime", None])
                comment = "Restarting part.  Previous data {}".format(existing)
                add_part_info(
                    session,
                    pn=pn,
                    comment=comment,
                    at_date=start_date.gps,
                    reference="cm_partconnect",
                )
            else:
                print_out = (
                    "No action. The request {} not an allowed part restart.".format(
                     pn
                    )
                )
                this_data = None
        if this_data is not None:
            data = data + this_data
        print(print_out.capitalize())

    _update_part(session, data)
    if close_session_when_done:  # pragma: no cover
        session.close()


def _update_part(session=None, data=None):
    """
    Update the database given a part number with columns/values.

    This is a low-level module, generally called from somewhere else

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    data : list of lists
        List containing the pn, column and value to update
            [[pn0,column0,value0],[...]]
                pnN:  hera part number as primary key
                columnN:  column name(s)
                valueN:  corresponding list of values

    Returns
    -------
    bool
        True if any updates were made, else False

    """

    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    made_change = False
    for this_entry in data:
        pn, col, val = this_entry
        part_rec = session.query(Parts).filter(
            (func.upper(Parts.pn) == pn.upper())
        )
        num_part = part_rec.count()
        if num_part == 0:
            part = Parts()
        elif num_part == 1:
            part = part_rec.first()
        try:
            try_col = getattr(part, col)  # noqa
            setattr(part, col, val)
        except AttributeError:
            print(col, "does not exist as a field")
            continue
        made_change = True
        session.add(part)
        session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()

    return made_change


class AprioriAntenna(MCDeclarativeBase):
    """
    Table for a priori antenna status.

    Parameters
    ----------
    antenna :  str
        antenna designation
    start_gpstime : int
        start time for antenna status
    stop_gpstime : int
        stop time for antenna status
    status :  str
        status - TBD
    """

    __tablename__ = "apriori_antenna"

    antenna = Column(Text, primary_key=True)
    start_gpstime = Column(BigInteger, primary_key=True)
    stop_gpstime = Column(BigInteger)
    status = Column(Text, nullable=False)

    def __repr__(self):
        """Define representation."""
        return "<{}: {}  [{} - {}]>".format(
            self.antenna, self.status, self.start_gpstime, self.stop_gpstime
        )

    def valid_statuses(self):
        """Define current valid statuses GET FROM SYSDEF.JSON!."""
        return [
            "DEFINE IN SYSDEF.JSON!",
            "active",
            "maintenance",
            "etc"
        ]


def get_allowed_apriori_antenna_statuses():
    """Get list of valid apriori statuses."""
    apa = AprioriAntenna()
    return apa.valid_statuses()


def update_apriori_antenna(
    antenna, status, start_gpstime, stop_gpstime=None, session=None
):
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
    new_apa = AprioriAntenna()

    if status not in new_apa.valid_statuses():
        raise ValueError(
            "Antenna apriori status must be in {}".format(new_apa.valid_statuses())
        )

    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    antenna = antenna.upper()
    last_one = 1000
    old_apa = None
    for trial in session.query(AprioriAntenna).filter(
        func.upper(AprioriAntenna.antenna) == antenna
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


class PartInfo(MCDeclarativeBase):
    """
    A table for logging test information etc for parts.

    Attributes
    ----------
    pn : String Column
        A system part number for each part.
    posting_gpstime : BigInteger Column
        Time that the data are posted. Part of the primary_key
    comment : String Column
        Comment associated with this data - or the data itself...
    reference : String Column
        Other reference associated with this entry.
    """

    __tablename__ = "part_info"

    pn = Column(String(64), nullable=False, primary_key=True)
    posting_gpstime = NotNull(BigInteger, primary_key=True)
    comment = NotNull(String(2048))
    reference = Column(String(256))

    def __repr__(self):
        """Define representation."""
        return (
            "<heraPartNumber id = {self.hpn} "
            "comment = {self.comment}>".format(self=self)
        )

    def gps2Time(self):
        """Add a posting_date attribute (astropy Time object) based on posting_gpstime."""
        self.posting_date = Time(self.posting_gpstime, format="gps")

    def info(self, **kwargs):
        """Add arbitrary attributes passed in a dict to this object."""
        for key, value in kwargs.items():
            setattr(self, key, value)


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
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    pi = PartInfo()
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


class Connections(MCDeclarativeBase):
    """
    A table for logging connections between parts.

    Part and Port must be unique when combined

    Attributes
    ----------
    upstream_part : String Column
        up refers to the skyward part,
        Signal flows from A->B"
        Part of the primary_key, Foreign Key into parts
    upstream_output_port : String Column
        connected output port on upstream (skyward) part.
        Part of the primary_key
    downstream_part : String Column
        down refers to the part that is further from the sky, e.g.
        Part of the primary_key, Foreign Key into parts
    downstream_input_port : String Column
        connected input port on downstream (further from the sky) part
        Part of the primary_key
    start_gpstime : BigInteger Column
        start_time is the time that the connection is set
        Part of the primary_key
    stop_gpstime : BigInteger Column
        stop_time is the time that the connection is removed

    """

    __tablename__ = "connections"

    upstream_part = Column(String(64), nullable=False, primary_key=True)
    upstream_output_port = NotNull(String(64), primary_key=True)
    downstream_part = Column(String(64), nullable=False, primary_key=True)
    downstream_input_port = NotNull(String(64), primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["upstream_part"], [Parts.pn]
        ),
        ForeignKeyConstraint(
            ["downstream_part"], [Parts.pn]
        ),
    )

    start_gpstime = NotNull(BigInteger, primary_key=True)
    stop_gpstime = Column(BigInteger)

    def __repr__(self):
        """Define representation."""
        up = "{self.upstream_part}".format(self=self)
        down = "{self.downstream_part}".format(self=self)
        return (
            "<{}<{self.upstream_output_port}|{self.downstream_input_port}>{}>".format(
                up, down, self=self
            )
        )

    def __eq__(self, other):
        """Define equality."""
        if (
            isinstance(other, self.__class__)
            and self.upstream_part.upper() == other.upstream_part.upper()
            and self.upstream_output_port.upper() == other.upstream_output_port.upper()
            and self.downstream_part.upper() == other.downstream_part.upper()
            and self.downstream_input_port.upper()
            == other.downstream_input_port.upper()
        ):
            return True
        return False

    def gps2Time(self):
        """
        Add start_date and stop_date attributes (astropy Time objects).

        Based on start_gpstime and stop_gpstime.
        """
        self.start_date = Time(self.start_gpstime, format="gps")
        if self.stop_gpstime is None:
            self.stop_date = None
        else:
            self.stop_date = Time(self.stop_gpstime, format="gps")

    def connection(self, **kwargs):
        """
        Add arbitrary attributes passed in a dict to this object.

        Allows arbitrary connection to be specified.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _to_dict(self):
        return {
            "upstream_part": self.upstream_part,
            "upstream_output_port": self.upstream_output_port,
            "downstream_part": self.downstream_part,
            "downstream_input_port": self.downstream_input_port,
        }


def get_connection_from_dict(input_dict):
    """
    Convert a dictionary holding the connection info into a Connections object.

    Parameter
    ---------
    input_dict : dictionary
        The dictionary must have the following keys:
            upstream_part, upstream_output_port
            downstream_part, downstream_input_port
        Other keys will be ignored.

    Returns
    -------
    Connections object

    """
    return Connections(
        upstream_part=input_dict["upstream_part"],
        upstream_output_port=input_dict["upstream_output_port"],
        downstream_part=input_dict["downstream_part"],
        downstream_input_port=input_dict["downstream_input_port"],
    )


def get_null_connection():
    """
    Return a null connection.

    All components hold the no_connection_designator and dates/times are None

    Returns
    -------
    Connections object

    """
    nc = no_connection_designator
    return Connections(
        upstream_part=nc,
        upstream_output_port=nc,
        downstream_part=nc,
        downstream_input_port=nc,
        start_gpstime=None,
        stop_gpstime=None,
    )


def stop_connections(session, conns, stop_dates):
    """
    Add a stop_date to the connections in conn_list.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    conns:  list of lists
        List with data [[upstream_part,port,downstream_part,port],...]
    stop_dates : list of astropy.Time
        date at which to stop connection

    """
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    data = []
    for conn, stop_date in zip(conns, stop_dates):
        conn_to_stop = session.query(Connections).filter(
            (Connections.upstream_part == conn[0])
            & (Connections.upstream_output_port == conn[1])
            & (Connections.downstream_part == conn[2])
            & (Connections.downstream_input_port == conn[3])
        )
        cdes = "{}<{} - {}>{}".format(conn[0], conn[1], conn[2], conn[3])
        num_conn = conn_to_stop.count()
        if num_conn == 0:
            print("Skipping: no connection listed for {}".format(cdes))
        else:
            fnd_conn = 0
            for chk_conn in conn_to_stop:
                if chk_conn.stop_gpstime is None:
                    this_one = []
                    for cc in conn:
                        this_one.append(cc)
                    this_one.append(chk_conn.start_gpstime)
                    this_one.append(stop_date.gps)
                    data.append(this_one)
                    print("Stopping {}:  {}".format(cdes, chk_conn.start_gpstime))
                    if fnd_conn:
                        print("\t***Warning - this is number {} for this connection! "
                              "Started at {}.  Should clean up."
                              .format(fnd_conn+1, chk_conn.start_gpstime))
                    fnd_conn += 1
            if not fnd_conn:
                print("Skipping: no unstopped connections for {}".format(cdes))

    _update_connection(session, data, False)
    if close_session_when_done:
        session.close()


def add_new_connections(session, conns, start_dates, same_conn_sec=60.0):
    """
    Add a new connection.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    conns:  list of lists
        List with data [[upstream_part,port,downstream_part,port],...]
    start_dates : list of astropy.Time
        date at which to start connection
    same_conn_sec

    """
    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    data = []
    for conn, start_date in zip(conns, start_dates):
        conn_to_start = session.query(Connections).filter(
            (Connections.upstream_part == conn[0])
            & (Connections.upstream_output_port == conn[1])
            & (Connections.downstream_part == conn[2])
            & (Connections.downstream_input_port == conn[3])
        )
        cdes = "{}<{} - {}>{}".format(conn[0], conn[1], conn[2], conn[3])
        add_this_one = True
        for chk_conn in conn_to_start:
            if abs(chk_conn.start_gpstime - start_date.gps) < same_conn_sec:
                print("{} is already a connection starting at {}"
                      .format(cdes, chk_conn.start_gpstime), end='')
                if chk_conn.stop_gpstime is None:
                    print(" --> Skipping.")
                    add_this_one = False
                else:
                    print("\twith a stop time of {}".format(chk_conn.stop_gpstime))
                break
        if add_this_one:
            print("Starting connection {} at {}".format(cdes, str(start_date)))
            this_one = []
            for cc in conn:
                this_one.append(cc)
            this_one.append(start_date.gps)
            this_one.append(None)
            data.append(this_one)

    _update_connection(session, data, True)
    if close_session_when_done:
        session.close()


def _update_connection(session=None, conns=None):
    """
    Update the database given a connection with columns/values.

    This assumes that the calling function has checked.

    Parameters
    ----------
    session : object
        Database session to use.  If None, it will start a new session, then close.
    data : list of lists
        Contains list of connections (all entries in each element)

    Returns
    -------
    bool
        True if succesful, otherwise False

    """

    close_session_when_done = False
    if session is None:  # pragma: no cover
        db = mc.connect_to_mc_db(None)
        session = db.sessionmaker()
        close_session_when_done = True

    for conn in conns:
        connection = Connections()
        connection.connection(
                    up=conn[0],
                    down=conn[1],
                    upstream_output_port=conn[2],
                    downstream_input_port=conn[3],
                    start_gpstime=conn[4],
                    stop_gpstime=conn[5]
                )
        session.add(connection)
        session.commit()
    if close_session_when_done:  # pragma: no cover
        session.close()

    return True
