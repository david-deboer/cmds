# -*- mode: python; coding: utf-8 -*-
# Copyright 2023 the David DeBoer
# Licensed under the 2-clause BSD license.

"""This is the base class for the script generators."""
import datetime
import os
from . import cm, cm_utils, cm_gsheet_ata, cm_active


def as_part(add_or_stop, p, cdate, ctime):
    """Return a string to use cmds script to add or stop a part."""
    s = f'cmds_update_part.py {add_or_stop} {p[0]} '
    if add_or_stop == 'add':
        s += f'-t {p[1]} -m {p[2]} '
    s += f'--date {cdate} --time {ctime}'
    return s


def as_connect(add_or_stop, up, dn, cdate, ctime):
    """Return a string to use cmds script to add or stop a connection."""
    s = 'cmds_update_connection.py {} -u {} --upport {} -d {} --dnport {}'\
        ' --date {} --time {}'.format(add_or_stop, up[0], up[1],
                                        dn[0], dn[1], cdate, ctime)
    return s


class Update():
    """Base update class."""

    def __init__(self, script_type, script_path=None, chmod=False, verbose=True, args=None):
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
        kwargs: None or Namespace
            If not None, can contain cm_config_path, cm_db_name

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
        db = cm.connect_to_cm_db(args)
        self.session = db.sessionmaker()
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
            self.script_path = ''
            self.fp = None
            return

        if script_path == 'default' or script_path is None:
            self.script_path = cm.get_script_path()
        else:
            self.script_path = script_path
        self.script = '{}_{}_{}'.format(self.cdate.replace('/', '')[2:], script_type,
                                        self.ctime.replace(':', ''))
        self.script = os.path.join(self.script_path, self.script)

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

    def load_active(self, loading=['parts', 'connections', 'stations', 'info', 'apriori']):
        """Load all active information."""
        self.active = cm_active.ActiveData(session=self.session, at_date=self.at_date)
        for param in loading:
            getattr(self.active, f"load_{param}")()

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

    def load_gsheet(self, split=True, arc_csv='none', tabs=None, path='', time_tag='_%y%m%d'):
        """Get the googlesheet information from the internet."""
        self.gsheet = cm_gsheet_ata.SheetData()
        self.gsheet.load_sheet(arc_csv=arc_csv, tabs=None, path=path, time_tag=time_tag)
        if split:
            self.gsheet.split_apriori()
            self.gsheet.split_comments()

    def finish(self, cronjob_script=None, archive_to=None):
        """
        Close out process.  If no updates, it deletes the script file.
        If both parameters are None, it just leaves things alone.

        Parameters
        ----------
        cronjob_script : str or None
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
        if cronjob_script is not None:
            cronjob_script = os.path.join(script_path, cronjob_script)
            if os.path.exists(cronjob_script):
                os.remove(cronjob_script)

        if self.update_counter == 0:  # No updates made
            if os.path.exists(self.script):
                os.remove(self.script)
            if self.verbose:
                print("No updates found.  Removing {}.".format(self.script))
            if cronjob_script is not None:
                with open(cronjob_script, 'w') as fp:
                    fp.write('\n')
                if self.verbose:
                    print("Writing empty {}.".format(cronjob_script))
        else:
            if archive_to is not None:
                os.system('cp {} {}'.format(self.script, archive_to))
                if self.verbose:
                    print("Copying {}  -->  {}".format(self.script, archive_to))
            if cronjob_script is not None:
                os.rename(self.script, cronjob_script)
                if self.chmod:
                    os.chmod(cronjob_script, 0o755)
                if self.verbose:
                    print("Moving {}  -->  {}".format(self.script, cronjob_script))

