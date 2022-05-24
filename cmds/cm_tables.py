# -*- mode: python; coding: utf-8 -*-
# Copyright 2022 David R. DeBoer
# Licensed under the 2-clause BSD license.

"""All of the tables defined here."""

from astropy.time import Time
from sqlalchemy import BigInteger, Column, ForeignKeyConstraint, String, Text, Float, func
from . import MCDeclarativeBase, NotNull, cm
import copy


def get_sstimes(cls):
    """Return formatted start/stop."""
    try:
        start = cls.start_date.datetime.isoformat()
    except AttributeError:
        start = str(cls.start_gpstime)
    if cls.stop_gpstime is None:
        stop = '_'
    else:
        try:
            stop = cls.stop_date.datetime.isoformat()
        except AttributeError:
            stop = str(cls.stop_gpstime)
    return f"[{start} - {stop}]"


def get_cptimes(cls, corp):
    """Return formatted created/posted."""
    cps = f"{corp}_date"
    try:
        return f"[{getattr(cls, cps).datetime.isoformat()}]"
    except AttributeError:
        cps = f"{corp}_gpstime"
    return f"[{getattr(cls, cps)}]"


class Stations(MCDeclarativeBase):
    """
    A table logging stations, which are geographical locations.

    Attributes
    ----------
    station_name : String Column
        Colloquial name of station (which is a unique location on the ground).
        This one shouldn't change. Primary_key
    station_type : String Column
        Type of station.
    datum : String Column
        Datum of the geoid.
    tile : String Column
        UTM tile
    northing : Float Column
        Northing coordinate in m
    easting : Float Column
        Easting coordinate in m
    elevation : Float Column
        Elevation in m
    created_gpstime : BigInteger Column
        The date when the station assigned by project.

    """

    __tablename__ = "stations"

    station_name = Column(String(64), primary_key=True)
    station_type = Column(String(64), nullable=False)
    datum = Column(String(64))
    tile = Column(String(64))
    northing = Column(Float(precision="53"))
    easting = Column(Float(precision="53"))
    elevation = Column(Float)
    created_gpstime = NotNull(BigInteger)

    def gps2Time(self):
        """Add a created_date attribute -- an astropy Time object based on created_gpstime."""
        self.created_date = Time(self.created_gpstime, format="gps")

    def station(self, **kwargs):
        """Allow specification of an arbitrary station value."""
        updated = 0
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'station_name':
                    value = value.upper()
                elif key == 'created_gpstime' and value is not None:
                    value = int(value)
                setattr(self, key, value)
                updated += 1
            else:
                print("{} is not a valid station attribute.".format(key))
                continue
        return updated

    def __repr__(self):
        """Define representation."""
        a = f"<Station: name={self.station_name} type={self.station_type} "
        b = f"northing={self.northing} easting={self.easting} "
        c = f"elevation={self.elevation}  {get_cptimes(self, 'created')}>"
        return a + b + c


def update_stations(stations, dates, session=None):
    """
    Add stations to Stations table.

    Parameters
    ----------
    stations : list of dicts
        dicts contain all Stations entries
    dates : list of astropy.Time objects
        List of dates to use for logging creation.
    session : session
        session on current database. If session is None, a new session
        on the default database is created and used.

    Returns
    -------
    int
        Number of attributes changed

    """

    updated = 0
    with cm.CMSessionWrapper(session) as session:
        for statd, date in zip(stations, dates):
            sn = statd['station_name'].upper()
            statx = session.query(Stations).filter(
                func.upper(Stations.station_name) == sn).first()
            if statx is None:
                station = Stations()
                updated += station.station(created_gpstime=date.gps, **statd)
                print(f"Add {station}")
                session.add(station)
            else:
                print(f"{sn} already present.  No action.")
    return updated


class Parts(MCDeclarativeBase):
    """
    A table logging parts within the system.

    Attributes
    ----------
    pn : String Column
        System part number for each part; part of the primary key.
    ptype : String Column
        A part-dependent string, i.e. feed, frontend, ...
    manufacturer_id : String Column
        A part number/serial number as specified by manufacturer
    start_gpstime : BigInteger Column
        The date when the part was installed (or otherwise assigned by project).
    stop_gpstime : BigInteger Column
        The date when the part was removed (or otherwise de-assigned by project).

    """

    __tablename__ = "parts"

    pn = Column(String(64), primary_key=True)
    ptype = NotNull(String(64))
    manufacturer_id = Column(String(64))
    start_gpstime = Column(BigInteger, nullable=False)
    stop_gpstime = Column(BigInteger)

    def __repr__(self):
        """Define representation."""
        return f"<Part: name={self.pn} type={self.ptype}  {get_sstimes(self)}>"

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
        """Allow specification of an arbitrary part value."""
        updated = 0
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'pn':
                    value = value.upper()
                elif key in ['start_gpstime', 'stop_gpstime']:
                    if value is not None:
                        value = int(value)
                setattr(self, key, value)
                updated += 1
            else:
                print("{} is not a valid part attribute.".format(key))
                continue
        return updated


def update_parts(parts, dates, session=None):
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

    Returns
    -------
    int
        Number of attributes changed.
    """
    updated = 0
    with cm.CMSessionWrapper(session) as session:
        for partd, date in zip(parts, dates):
            pn = partd['pn'].upper()
            part = session.query(Parts).filter(func.upper(Parts.pn) == pn).first()
            if partd['action'].lower() == 'stop':
                this_update = None
                if part is None:
                    print(f"{pn} is not in database.  No update.")
                elif part.stop_gpstime is not None:
                    print(f"{pn} already has a stop time ({part.stop_gpstime}).  No update.")
                else:
                    this_update = {'stop_gpstime': date.gps}
            elif partd['action'].lower() == 'add':
                this_update = {'pn': pn, 'ptype': partd['ptype'], 'manufacturer_id': partd['manufacturer_id'],
                               'start_gpstime': date.gps, 'stop_gpstime': None}
                if part is None:
                    part = Parts()
                else:
                    print(f"{pn} already in database.  No update.")
                    this_update = None
            if this_update is not None:
                updated += part.part(**this_update)
                print(f"{partd['action']} {part}")
                session.add(part)
    return updated


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

    __table_args__ = (
        ForeignKeyConstraint(
            ["pn"], [Parts.pn]
        ),
    )

    def __repr__(self):
        """Define representation."""
        return f"<Info:  name={self.pn} comment = {self.comment}  {get_cptimes(self, 'posting')}>"

    def gps2Time(self):
        """Add a posting_date attribute (astropy Time object) based on posting_gpstime."""
        self.posting_date = Time(self.posting_gpstime, format="gps")

    def info(self, **kwargs):
        """Add arbitrary attributes passed in a dict to this object."""
        updated = 0
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key == 'pn':
                    value = value.upper()
                elif key == 'posting_gpstime':
                    value = int(value)
                setattr(self, key, value)
                updated += 1
            else:
                print(f"{key} is not a valid part_info attribute.")
        return updated


def update_info(infos, dates, session):
    """
    Add part information into database.

    Parameters
    ----------
    infos : list of dicts
        Dicts contain the part info.
    dates : list of astropy.Time objects
        Date for comments.
    session : object
        Database session to use.  If None, it will start a new session, then close.

    Returns
    =======
    int
        Number of attributes changed.
    """
    updated = 0
    with cm.CMSessionWrapper(session) as session:
        for infod, date in zip(infos, dates):
            pn = infod['pn'].upper()
            infox = session.query(PartInfo).filter((func.upper(PartInfo.pn) == pn)
                                                   & (PartInfo.posting_gpstime
                                                      == date.gps)).first()
            if infox is None:
                info = PartInfo()
                updated += info.info(posting_gpstime=date.gps, **infod)
                print(f"Add {info}")
                session.add(info)
            else:
                print(f"{pn} already has info at this time.  No action.")
    return updated


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

    no_connection_designator = "-X-"

    def __repr__(self):
        """Define representation."""
        up, down = f"{self.upstream_part}", f"{self.downstream_part}"
        uport, dport = f"{self.upstream_output_port}", f"{self.downstream_input_port}"
        return f"<Connection: {up}<{uport}|{dport}>{down}  {get_sstimes(self)}>"

    def __eq__(self, other):
        """Define equality."""
        if (
            isinstance(other, self.__class__)
            and self.upstream_part.upper() == other.upstream_part.upper()
            and self.upstream_output_port.lower() == other.upstream_output_port.lower()
            and self.downstream_part.upper() == other.downstream_part.upper()
            and self.downstream_input_port.lower() == other.downstream_input_port.lower()
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
        """Allow specification of arbitrary connection."""
        updated = 0
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key in ['upstream_part', 'downstream_part']:
                    value = value.upper()
                elif key in ['upstream_output_port', 'downstream_input_port']:
                    value = value.lower()
                elif key in ['start_gpstime', 'stop_gpstime']:
                    if value is not None:
                        value = int(value)
                setattr(self, key, value)
                updated += 1
            else:
                print("{} is not a valid connection entry.".format(key))
                continue
        return updated


def get_connection(conn):
    """
    Return a connection.

    All components hold the no_connection_designator and dates/times are None

    Parameters
    ----------
    conn : dict or None
        If None, return "no_connection"

    Returns
    -------
    Connections object

    """
    if conn is None:
        return Connections(
            upstream_part=Connections.no_connection_designator,
            upstream_output_port=Connections.no_connection_designator,
            downstream_part=Connections.no_connection_designator,
            downstream_input_port=Connections.no_connection_designator,
            start_gpstime=None,
            stop_gpstime=None,
        )
    this_conn = Connections()
    this_conn.connection(**conn)
    return this_conn


def update_connections(conns, dates, same_conn_sec=10, session=None):
    """
    Add or stop connections.

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

    Returns
    -------
    int
        Number of attributes changed.

    """
    updated = 0
    with cm.CMSessionWrapper(session) as session:
        for connd, date in zip(conns, dates):
            connections_to_check = session.query(Connections).filter(
                (func.upper(Connections.upstream_part) == connd['upstream_part'].upper())
                & (func.lower(Connections.upstream_output_port)
                   == connd['upstream_output_port'].lower())
                & (func.upper(Connections.downstream_part) == connd['downstream_part'].upper())
                & (func.lower(Connections.downstream_input_port)
                   == connd['downstream_input_port'].lower())
            )
            if connd['action'].lower() == 'stop':
                this_update = None
                if connections_to_check.count() == 0:
                    print(f"No connection in database {connd}.")
                else:
                    for connx in connections_to_check:
                        if connx.stop_gpstime is None:
                            if this_update is None:
                                this_update = {'stop_gpstime', date.gps}
                                connection = copy(connx)
                            else:
                                print(f"Multiple open connections for {connx}. No action.")
                                this_update = None
                                break
            elif connd['action'].lower() == 'add':
                this_update = {"upstream_part": connd['upstream_part'],
                               "upstream_output_port": connd['upstream_output_port'],
                               "downstream_part": connd['downstream_part'],
                               "downstream_input_port": connd['downstream_input_port'],
                               "start_gpstime": date.gps, "stop_gpstime": None}
                if connections_to_check.count() == 0:
                    connection = Connections()
                else:
                    for connection in connections_to_check:
                        if abs(connection.start_gpstime - date.gps) < same_conn_sec:
                            print(f"{connection} is already present.  No action.")  # noqa
                            this_update = None
                    connection = Connections()
            if this_update is not None:
                updated += connection.connection(**this_update)
                print(f"{connd['action']} {connection}")
                session.add(connection)
    return updated


class AprioriStatus(MCDeclarativeBase):
    """
    Table for a priori status (generally antenna-based).

    Parameters
    ----------
    pn :  str
        part number
    start_gpstime : int
        start time for status
    stop_gpstime : int
        stop time for status
    status :  str
        status - TBD
    comment : str
        comment
    """

    __tablename__ = "apriori_status"

    pn = Column(Text, primary_key=True)
    start_gpstime = Column(BigInteger, primary_key=True)
    stop_gpstime = Column(BigInteger)
    status = Column(Text, nullable=False)
    comment = Column(Text)

    __table_args__ = (
        ForeignKeyConstraint(
            ["pn"], [Parts.pn]
        ),
    )

    def __repr__(self):
        """Define representation."""
        return f"<Apriori: name={self.pn} status={self.status} {get_sstimes(self)}>"

    def apriori(self, **kwargs):
        """Add arbitrary attributes passed in a dict to this object."""
        updated = 0
        for key, value in kwargs.items():
            if hasattr(self, key):
                if key in ['start_gpstime', 'stop_gpstime']:
                    if value is not None:
                        value = int(value)
                setattr(self, key, value)
                updated += 1
            else:
                print(f"{key} is not a valid apriori_status attribute.")
        return updated

    def valid_statuses(self):
        """Get defined current valid statuses."""
        from . import cm_sysdef
        apstat = cm_sysdef.Sysdef(None)
        return apstat.apriori_statuses


def get_allowed_apriori_statuses():
    """Get list of valid apriori statuses."""
    aps = AprioriStatus()
    return aps.valid_statuses()


def update_aprioris(aprioris, dates, session=None):
    """
    Add or stop apriori statuses.

    Parameters
    ----------
    aprioris : list of dicts
        List of dicts containing apriori information and action (add/stop)
    dates : list of astropy.Time objects
        List of dates to use for logging the add/stop.
    session : object
        Database session to use.  If None, it will start a new session, then close.

    Returns
    -------
    int
        Number of attributes changed.

    """
    updated = 0
    with cm.CMSessionWrapper(session) as session:
        for apriorid, date in zip(aprioris, dates):
            pn = apriorid['pn'].upper()
            aprioric = session.query(AprioriStatus).filter(func.upper(AprioriStatus.pn) == pn)
            for apx in aprioric:  # Stop all old ones.
                if apx.stop_gpstime is None:
                    updated += apx.apriori(stop_gpstime=date.gps)
                    print(f"Stop {apx}")
                    session.add(apx)
            if apriorid['action'].lower() == 'stop':
                if aprioric.count() == 0:
                    print(f"{pn} is not in database.  No update.")
            elif apriorid['action'].lower() == 'add':
                this_update = {'pn': pn, 'status': apriorid['status'], 'comment': apriorid['comment'],
                               'start_gpstime': date.gps, 'stop_gpstime': None}
                apriori = AprioriStatus()
                updated += apriori.apriori(**this_update)
                print(f"Add {apriori}")
                session.add(apriori)
    return updated
