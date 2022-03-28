# -*- mode: python; coding: utf-8 -*-
# Copyright 2017 the HERA Collaboration
# Licensed under the 2-clause BSD license.
"""
Primary session object which handles most DB queries.

See INSTALL.md in the Git repository for instructions on how to initialize
your database and configure M&C to find it.
"""

from sqlalchemy import desc, asc
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func
from astropy.time import Time


class MCSession(Session):
    """Primary session object that handles most DB queries."""

    def __enter__(self):
        """Enter the session."""
        return self

    def __exit__(self, etype, evalue, etb):
        """Exit the session, rollback if there's an error otherwise commit."""
        if etype is not None:
            self.rollback()  # exception raised
        else:
            self.commit()  # success
        self.close()
        return False  # propagate exception if any occurred

    def get_current_db_time(self):
        """
        Get the current time according to the database.

        Returns
        -------
        astropy Time object
            Current database time as an astropy time object.

        """
        db_timestamp = self.execute(func.current_timestamp()).scalar()

        # convert to astropy time object
        db_time = Time(db_timestamp)
        return db_time

    def _time_filter(
        self,
        table_class,
        time_column,
        most_recent=None,
        starttime=None,
        stoptime=None,
        filter_column=None,
        filter_value=None,
        write_to_file=False,
        filename=None,
    ):
        """
        Fiter entries by time, used by most get methods on this object.

        Default behavior is to return the most recent record(s) -- there can be
        more than one if there are multiple records at the same time. If
        starttime is set but stoptime is not, this method will return the first
        record(s) after the starttime -- again there can be more than one if
        there are multiple records at the same time. If you want a range of
        times you need to set both startime and stoptime. If most_recent is set,
        startime and stoptime are ignored.

        Parameters
        ----------
        table_class : class
            Class specifying a table to query.
        time_column : str
            column name holding the time to filter on.
        most_recent : bool
            Option to get the most recent record(s). Defaults to True if
            starttime is None.
        starttime : astropy Time object
            Time to look for records after. Ignored if most_recent is True,
            required if most_recent is False.
        stoptime : astropy Time object
            Last time to get records for, only used if starttime is not None.
            If none, only the first record(s) after starttime will be returned
            (can be more than one record if multiple records share the same
            time). Ignored if most_recent is True.
        filter_column : str or list of str
            Column name(s) to use as an additional filter (often a part of the
            primary key).
        filter_value : str or int or list of str or int
            Type coresponds to filter_column(s), value(s) to require
            that the filter_column(s) are equal to.
        write_to_file : bool
            Option to write records to a CSV file.
        filename : str
            Name of file to write to. If not provided, defaults to a file in the
            current directory named based on the table name.
            Ignored if write_to_file is False.

        Returns
        -------
        list of objects, optional
            If write_to_file is False: List of objects that match the filtering.

        """
        if starttime is None and most_recent is None:
            most_recent = True

        if not isinstance(most_recent, (type(None), bool)):
            raise TypeError("most_recent must be None or a boolean")

        if not most_recent:
            if starttime is None:
                raise ValueError(
                    "starttime must be specified if most_recent " "is False"
                )
            if not isinstance(starttime, Time):
                raise ValueError(
                    "starttime must be an astropy time object. "
                    "value was: {t}".format(t=starttime)
                )

        if stoptime is not None:
            if not isinstance(stoptime, Time):
                raise ValueError(
                    "stoptime must be an astropy time object. "
                    "value was: {t}".format(t=stoptime)
                )

        time_attr = getattr(table_class, time_column)

        if filter_value is not None:
            if isinstance(filter_column, (list)):
                assert isinstance(filter_value, (list)), (
                    f"Inconsistent filtering keywords for {table_class.__tablename__} "
                    "table. This is a bug, please report it in the issue log."
                )
                assert len(filter_column) == len(filter_value), (
                    f"Inconsistent filtering keywords for {table_class.__tablename__} "
                    "table. This is a bug, please report it in the issue log."
                )
            else:
                filter_column = [filter_column]
                filter_value = [filter_value]
            filter_attr = []
            for col in filter_column:
                filter_attr.append(getattr(table_class, col))

        query = self.query(table_class)
        if filter_value is not None:
            for index, val in enumerate(filter_value):
                if val is not None:
                    query = query.filter(filter_attr[index] == val)

        if most_recent or stoptime is None:
            if most_recent:
                current_time = Time.now()
                # get most recent row
                first_query = (
                    query.filter(time_attr <= current_time.gps)
                    .order_by(desc(time_attr))
                    .limit(1)
                )
            else:
                # get first row after starttime
                first_query = (
                    query.filter(time_attr >= starttime.gps)
                    .order_by(asc(time_attr))
                    .limit(1)
                )

            # get the time of the first row
            first_result = first_query.all()
            if len(first_result) < 1:
                query = first_query
            else:
                first_time = getattr(first_result[0], time_column)
                # then get all results at that time
                query = query.filter(time_attr == first_time)
                if filter_value is not None:
                    for attr in filter_attr:
                        query = query.order_by(asc(attr))

        else:
            query = query.filter(time_attr.between(starttime.gps, stoptime.gps))
            query = query.order_by(time_attr)
            if filter_value is not None:
                for attr in filter_attr:
                    query = query.order_by(asc(attr))

        if write_to_file:
            self._write_query_to_file(query, table_class, filename=filename)
        else:
            return query.all()

    def _insert_ignoring_duplicates(self, table_class, obj_list, update=False):
        """
        Insert record handling duplication based on update flag.

        If the current database is PostgreSQL, this function will use a
        special insertion method that will ignore or update records that are
        redundant with ones already in the database. This makes it convenient to
        sample certain data (especially redis data) densely on qmaster or to
        update an existing record.

        Parameters
        ----------
        table_class : class
            Class specifying a table to insert into.
        obj_list : list of objects
            List of objects (of class table_class) to insert into the table.
        update : bool
            If true, update the existing record with the new data, otherwise do
            nothing (which is appropriate if the data is the same because of
            dense sampling).

        """
        if self.bind.dialect.name == "postgresql":
            from sqlalchemy import inspect
            from sqlalchemy.dialects.postgresql import insert

            ies = [c.name for c in inspect(table_class).primary_key]
            conn = self.connection()

            for obj in obj_list:
                # This appears to be the most correct way to map each row
                # object into a dictionary:
                values = {}
                for col in inspect(obj).mapper.column_attrs:
                    values[col.expression.name] = getattr(obj, col.key)

                if update:
                    # create dict of columns to update (everything other than
                    # the primary keys)
                    update_dict = {}
                    for col, val in values.items():
                        if col not in ies:
                            update_dict[col] = val

                    # The special PostgreSQL insert statement lets us update
                    # existing rows via `ON CONFLICT ... DO UPDATE` syntax.
                    stmt = (
                        insert(table_class)
                        .values(**values)
                        .on_conflict_do_update(index_elements=ies, set_=update_dict)
                    )
                else:
                    # The special PostgreSQL insert statement lets us ignore
                    # existing rows via `ON CONFLICT ... DO NOTHING` syntax.
                    stmt = (
                        insert(table_class)
                        .values(**values)
                        .on_conflict_do_nothing(index_elements=ies)
                    )
                conn.execute(stmt)
        else:  # pragma: no cover
            # Generic approach:
            for obj in obj_list:
                self.add(obj)
