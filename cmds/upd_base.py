# -*- mode: python; coding: utf-8 -*-
# Copyright 2023 the David DeBoer
# Licensed under the 2-clause BSD license.

"""This is the base class for the script generators."""
import datetime
import os
from . import cm, cm_utils, cm_gsheet_ata, cm_active, cm_handling


class Update():
    """Base update class."""

    def __init__(self, script_type, script_path=None, chmod=False, verbose=True):
        """
        Initialize.

        Parameters
        ----------
        script_type : str
            Type of the script to generate (becomes component of dated name).  If None print to screen only.
        script_path : str
            Path to the script.
        verbose : bool
            Verbose or not.

        """
        self.chmod = chmod
        self.verbose = verbose
        # Time glob
        self.now = datetime.datetime.now()
        self.cdate = self.now.strftime('%Y/%m/%d')
        self.ctime = self.now.strftime('%H:%M')
        time_offset = self.now + datetime.timedelta(seconds=100)
        self.cdate2 = time_offset.strftime('%Y/%m/%d')
        self.ctime2 = time_offset.strftime('%H:%M')
        self.at_date = cm_utils.get_astropytime(self.now)
        # Miscellaneous glob
        self.update_counter = 0
        self.gsheet = None
        self.part_prefix = cm_utils.PartPrefixMap()
        db = cm.connect_to_cm_db(None)
        self.session = db.sessionmaker()
        self.load_active()
        self.handle = cm_handling.Handling(session=self.session)
        self.script_setup(script_type=script_type, script_path=script_path)

    def script_setup(self, script_type, script_path=None):
        """
        Set up script name, pointer and initialize script.
        
        Parameters
        ----------
        script_type : str
            Type of the script to generate (becomes component of dated name).  If None print to screen only.
        script_path : str
            Path to the script.

        """
        if script_type is None:
            print("No script defined -- print to screen.")
            self.script = None
            self.fp = None
            return

        if script_path == 'default' or script_path is None:
            script_path = cm.get_script_path()
        self.script = '{}_{}_{}'.format(self.cdate.replace('/', '')[2:], script_type,
                                        self.ctime.replace(':', ''))
        self.script = os.path.join(script_path, self.script)

        if self.verbose:
            print(f"Writing script {self.script}")
        self.fp = open(self.script, 'w')
        s = '#! /bin/bash\n'
        unameInfo = os.uname()
        if unameInfo.sysname == 'Linux':
            s += 'source ~/.bashrc\n'
        self.fp.write(s)
        if self.verbose:
            print('-----------------')

    def load_active(self):
        """Load all active information."""
        self.active = cm_active.ActiveData(session=self.session, at_date=self.at_date)
        self.active.load_parts()
        self.active.load_connections()
        self.active.load_stations()
        self.active.info = []
        self.active.geo = []

    def printit(self, value):
        """Write as comment only."""
        if self.fp is None:
            print(value)
        else:
            print(value, file=self.fp)

    def no_op_comment(self, comment):
        """Write as comment only."""
        self.printit(f"# {comment.strip()}")

    def to_implement(self, command, ant, rev, statement, pdate, ptime):
        """Write generic 'to_implement' line."""
        stmt = "{} not implemented! {} {} {} {} {}\n".format(command, ant, rev,
                                                             statement, pdate, ptime)
        self.printit(stmt)

    def update__at_date(self, cdate='now', ctime='10:00'):
        """Set class date variable."""
        self.at_date = cm_utils.get_astropytime(adate=cdate, atime=ctime)

    def load_gsheet(self, node_csv='none', tabs=None, path='', time_tag='_%y%m%d'):
        """Get the googlesheet information from the internet."""
        self.gsheet = cm_gsheet_ata.SheetData()
        self.gsheet.load_sheet(node_csv=node_csv, tabs=None, path=path, time_tag=time_tag)

    def finish(self, cron_script=None, archive_to=None):
        """
        Close out process.  If no updates, it deletes the script file.
        If both parameters are None, it just leaves things alone.

        Parameters
        ----------
        cron_script : str or None
            If str, copies the script file to that.  Assumes in same directory as script.
            If no updates in script, then makes empty one.
        archive_to : str or None
            If str, moves the script file to that directory.  If not, deletes.
        """
        if self.fp is None:
            return
        self.fp.close()
        if self.verbose:
            print("----------------------DONE-----------------------")
        if not self.chmod:
            if self.verbose:
                print("If changes OK, 'chmod u+x {}' and run that script."
                      .format(self.script))
        else:
            os.chmod(self.script, 0o755)
            if self.verbose:
                print(f"Run {self.script}")
        script_path = os.path.dirname(self.script)
        if cron_script is not None:
            cron_script = os.path.join(script_path, cron_script)
            if os.path.exists(cron_script):
                os.remove(cron_script)

        if self.update_counter == 0:
            os.remove(self.script)
            if self.verbose:
                print("No updates found.  Removing {}.".format(self.script))
            if cron_script is not None:
                with open(cron_script, 'w') as fp:
                    fp.write('\n')
                if self.verbose:
                    print("Writing empty {}.".format(cron_script))
        else:
            if archive_to is not None:
                os.system('cp {} {}'.format(self.script, archive_to))
                if self.verbose:
                    print("Copying {}  -->  {}".format(self.script, archive_to))
            if cron_script is not None:
                os.rename(self.script, cron_script)
                if self.verbose:
                    print("Moving {}  -->  {}".format(self.script, cron_script))
            else:
                os.remove(self.script)

        if os.path.exists(cron_script):
            os.chmod(cron_script, 0o755)
