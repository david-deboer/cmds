"""Various signal chain modification methods."""
from . import cm, cm_utils, cm_active, cm_handling, upd_util
import os, json


with open(cm.file_finder('sysdef.json'), 'r') as fp:
    sysdef = json.load(fp)
PART_TYPES = {}
tmp = {}
for ptype, pdict in sysdef['components'].items():
    pp = pdict['prefix']
    PART_TYPES[pp] = ptype
    tmp.setdefault(len(pp), [])
    tmp[len(pp)].append(pp)
PART_TYPE_ORDER = []
for lpp in sorted(tmp, reverse=True):
    for pp in tmp[lpp]:
        PART_TYPE_ORDER.append(pp)

def get_part_type(prefix):
    for ppre in PART_TYPE_ORDER:
        if prefix.startswith(ppre):
            return PART_TYPES[ppre]
    raise ValueError(f"{prefix} not found in part types")


def as_part(add_or_stop, p, cdate, ctime):
    """Return a string to use cmds script to add or stop a part."""
    s = '{}_part.py -p {} '.format(add_or_stop, p[0])
    if add_or_stop == 'add':
        s += '-t {} -m {} '.format(p[1], p[2])
    s += '--date {} --time {}'.format(cdate, ctime)
    s += '\n'
    return s


def as_connect(add_or_stop, up, dn, cdate, ctime):
    """Return a string to use cmds script to add or stop a connection."""
    s = '{}_connection.py -u {} --upport {} -d {} --dnport {}'\
        ' --date {} --time {}\n'.format(add_or_stop, up[0], up[1],
                                        dn[0], dn[1], cdate, ctime)
    return s


class Update:
    """Holds the various update methods."""

    snap_ports = [{'e': 'e2', 'n': 'n0'}, {'e': 'e6', 'n': 'n4'}, {'e': 'e10', 'n': 'n8'}]

    def __init__(self, bash_script_name=None, chmod=False,
                 cdate='now', ctime='10:00',
                 log_file='scripts.log', verbose=True):
        """
        Initialize.

        Parameters
        ----------
        bash_script_name : str
            the name of the script to write (is executed outside of this)
        log_file : str
            name of log_file
        """
        self.verbose = verbose
        self.chmod = chmod
        self.log_file = log_file
        self.at_date = cm_utils.get_astropytime(cdate, ctime)
        db = cm.connect_to_cm_db(None)
        self.session = db.sessionmaker()
        self.active = cm_active.ActiveData(session=self.session)
        self.load_active(cdate=None)
        self.handle = cm_handling.Handling(session=self.session)
        self.script_setup(bash_script_name)

    def script_setup(self, bash_script_name):
        if bash_script_name is None:
            print("No script file started -- will print to screen.")
            self.fp = None
            self.bash_script_name = None
        else:
            if self.verbose:
                print("Writing script {}".format(bash_script_name))
            self.fp = open(bash_script_name, 'w')
            s = '#! /bin/bash\n'
            unameInfo = os.uname()
            if unameInfo.sysname == 'Linux':
                s += 'source ~/.bashrc\n'
            s += 'echo "{}" >> {} \n'.format(bash_script_name, self.log_file)
            self.fp.write(s)
            if self.verbose:
                print('-----------------')
        self.bash_script_name = bash_script_name

    # THESE ARE NEW COMPONENTS - eventually break out with a parent class
    # General order:
    #   add_antenna_station : when it is built
    #   add_node            : when all equipment installed in node
    #   add_antenna_to_node : when a feed/fem etc is installed and hooked into node

    def update__at_date(self, cdate='now', ctime='10:00'):
        """Set class date variable."""
        self.at_date = cm_utils.get_astropytime(adate=cdate, atime=ctime)

    def printit(self, value):
        """Write as comment only."""
        if self.fp is None:
            print(value)
        else:
            print(value, file=self.fp)

    def no_op_comment(self, comment):
        """Write as comment only."""
        self.printit(f"# {comment.strip()}")

    def load_active(self, cdate='now', ctime='10:00'):
        """Load all active information."""
        if cdate is None:
            at_date = self.at_date
        else:
            at_date = cm_utils.get_astropytime(cdate, ctime)
        self.active.load_parts(at_date=at_date)
        self.active.load_connections(at_date=None)
        self.active.info = []
        self.active.geo = []

    def add_antenna_station(self, stn, ser_num, cdate, ctime='10:00'):
        """
        Add an antenna station to the database.

        Parameters
        ----------
        stn : string or int
              antenna number (digits only)
        ser_num : string or int
                  installation order number of antenna
        cdate : string
                YYYY/MM/DD format
        ctime : string
                HH:MM format
        """
        added = {'station': [], 'part': [], 'connection': []}
        added['time'] = str(int(cm_utils.get_astropytime(cdate, ctime).gps))
        s = upd_util.gen_hpn('station', stn)
        a = upd_util.gen_hpn('antenna', stn)
        n = "H{}".format(ser_num)
        self.fp.write('add_station.py {} --sernum {} --date {} --time {}\n'
                      .format(s, ser_num, cdate, ctime))
        added['station'].append([s, added['time']])
        added['part'].append([s, 'A', 'station', n, added['time']])
        check_date = cm_utils.get_astropytime(adate=cdate, atime=ctime)

        if not self.exists('part', a, 'H'):
            ant = [a, 'H', 'antenna', n]
            self.update_part('add', ant, cdate, ctime)
            ant.append(added['time'])
            added['part'].append(ant)
        else:
            print("Part {} is already added".format(a))

        up = [s, 'A', 'ground']
        down = [a, 'H', 'ground']
        if not self.exists('connection', hpn=s, rev='A', port='ground', side='up',
                           check_date=check_date):
            self.update_connection('add', up, down, cdate, ctime)
            conn = [up[0], up[1], down[0], down[1], up[2], down[2], added['time']]
            added['connection'].append(conn)
        return added

    def stop_active_connections(self, hpn, rev='A', cdate='now', ctime='10:00', add_template=True):
        """
        Write script to stop all active connections of a part.

        Parameters
        ----------
        hpn : str
            HERA Part Number
        rev : str
            Revision
        cdate : *
            Date to use for stop.  Anything readable by cm_getastropytime.
        ctime : *
            Time to use for stop.  Depends on cdate
        add_template : bool
            Boolean to include same info but as "add" to help in starting new
        """
        stop_time = cm_utils.get_astropytime(adate=cdate, atime=ctime)
        cdate = stop_time.datetime.strftime('%Y/%m/%d')
        ctime = stop_time.datetime.strftime('%H:%M')
        hpnr = cm_utils.make_part_key(hpn, rev)
        print("Stopping connections")
        for key, conn in self.active.connections['up'][hpnr].items():
            print(conn)
            up = [conn.upstream_part, conn.up_part_rev, conn.upstream_output_port]
            dn = [conn.downstream_part, conn.down_part_rev, conn.downstream_input_port]
            self.update_connection('stop', up, dn, cdate, ctime)
        for key, conn in self.active.connections['down'][hpnr].items():
            print(conn)
            up = [conn.upstream_part, conn.up_part_rev, conn.upstream_output_port]
            dn = [conn.downstream_part, conn.down_part_rev, conn.downstream_input_port]
            self.update_connection('stop', up, dn, cdate, ctime)
        if add_template:
            print("Including add connection template -- EDIT!")
            self.fp.write("\n\nNeed to edit below!!!\n")
            for key, conn in self.active.connections['up'][hpnr].items():
                up = [conn.upstream_part, conn.up_part_rev, conn.upstream_output_port]
                dn = [conn.downstream_part, conn.down_part_rev, conn.downstream_input_port]
                self.update_connection('add', up, dn, cdate, ctime)
            for key, conn in self.active.connections['down'][hpnr].items():
                up = [conn.upstream_part, conn.up_part_rev, conn.upstream_output_port]
                dn = [conn.downstream_part, conn.down_part_rev, conn.downstream_input_port]
                self.update_connection('add', up, dn, cdate, ctime)

    def get_general_part(self, pn, at_date=None):
        """
        Return a list to add to script if part does not exist.

        Will return None if it does (or rev/type can't be found)
        """
        ptype = None
        for key, val in part_types.items():
            if pn.startswith(key):
                ptype = val
                break
        if ptype is None:
            return None
        if self.exists('part', pn=pn, port=None, side='up,down', check_date=at_date):
            return None
        return (pn, ptype, pn)

    def get_ser_num(self, hpn, part_type):
        """
        Pull the serial number out of the class dictionary.

        If none, just use the hpn
        """
        sn = hpn
        if part_type in self.ser_num_dict.keys():
            sn = self.ser_num_dict[part_type]
        return sn

    def to_implement(self, command, ant, rev, statement, pdate, ptime):
        """Write generic 'to_implement' line."""
        stmt = "{} not implemented! {} {} {} {} {}\n".format(command, ant, rev,
                                                             statement, pdate, ptime)
        self.printit(stmt)

    def update_part(self, add_or_stop, part, cdate, ctime):
        """
        Write appropriate entry for cmds script.

        Parameters
        ----------
        add_or_stop:  'add' or 'stop'
        part:  [hpn, rev, <type>, <mfg num>] (last two only for 'add')
        cdate:  date of update YYYY/MM/DD
        ctime:  time of update HH:MM
        """
        self.printit(as_part(add_or_stop, part, cdate, ctime))

    def update_connection(self, add_or_stop, up, down, cdate, ctime):
        """
        Write appropriate entry for cmds script.

        Parameters
        ----------
        add_or_stop:  'add' or 'stop'
        up:  upstream connection [part, rev, port]
        down:  downstream connection [part, rev, port]
        cdate:  date of update YYYY/MM/DD
        ctime:  time of update HH:MM
        """
        self.printit(as_connect(add_or_stop, up, down, cdate, ctime))

    def exists(self, atype, hpn, rev, port=None, side='up,down', check_date=None):
        """
        Check if a part or connection exists for hpn.

        Parameters
        ----------
        atype :  str
              'part' or 'connection'
        hpn : str
              HERA part number to check.  Can be a list of strings or a csv-list
        rev : str, list or None
              Revisions to check.  If None checks if any rev present.
        port : str, list or None
               Ports to check.  If None checks if any port is present.
        side : str
               "side" of part to check.  Options are:  up, down, or up,down (default)
        check_date : anything for cm_utils.get_astropytime
                     if None, it will warn if class and active don't agree (arb 1sec)
                     if not None, it will raise error if method and active don't agree (arb 1sec)
        Return
        ------
        boolean
                 True if existing corresponding hpn/rev/port
        """
        # if check_date is None:
        #     if abs(self.at_date - self.active.at_date) > 1.0:
        #         print("Warning:  class and active dates do not agree.")
        # else:
        #     if cm_utils.get_astropytime(check_date) - self.active.at_date < 0.0:
        #         raise ValueError("Supplied date before active.at_date.")
        if rev is None:
            rev = cm_active.revs(hpn)
        elif isinstance(rev, str):
            rev = [rev]
        active_part = False
        for r in rev:
            part_key = cm_utils.make_part_key(hpn, r)
            if part_key in self.active.parts.keys():
                active_part = True
                break
        if atype.startswith('part') or not active_part:
            return active_part

        if port is not None:
            sides = side.split(',')
            if isinstance(port, str):
                port = [port]
            for r in rev:
                part_key = cm_utils.make_part_key(hpn, r)
                for side in sides:
                    if part_key in self.active.connections[side].keys():
                        if port is None:
                            return True
                        for p in port:
                            if p.upper() in self.active.connections[side][part_key].keys():
                                return True
            return False

    def update_apriori(self, antenna, status, cdate, ctime='12:00'):
        """
        Update the antenna a priori status.

        Parameters
        ----------
        antenna : str
            Antenna part number, e.g. HH24
        status : str
            Antenna apriori enum string.
        cdate : str
            YYYY/MM/DD
        ctime : str, optional
            HH:MM, default is 12:00
        """
        self.printit('update_apriori.py -p {} -s {} --date {} --time {}\n'
                      .format(antenna, status, cdate, ctime))

    def add_part_info(self, hpn, rev, note, cdate, ctime, ref=None):
        """
        Add a note/comment for a part to the database.

        Parameters
        ----------
        hpn : str
              HERA part number for comment
        rev : str
              Revision for comment.
        note : str
               The desired note.
        cdate : str
                YYYY/MM/DD format
        ctime : str
                HH:MM format
        ref : str
              Reference note.
        """
        if not len(note.strip()):
            return
        if ref is None:
            ref = ''
        else:
            ref = '-l "{}" '.format(ref)
        self.printit("add_part_info.py -p {} -r {} -c '{}' {}--date {} --time {}\n"
                      .format(hpn, rev, note, ref, cdate, ctime))

    def replace(self, old, new, cdate, ctime='13:00'):
        """
        Replace an old PAM, FEM or SNAP part, with a new one.

        If new is None, it just stops the old one.

        Parameters
        ----------
        old : list
            [old_hpn, old_rev]
        new : list
            [new_hpn, old_rev]
        cdate : str
            YYYY/MM/DD
        ctime : str, optional
            HH:MM, default is 13:00
        """
        ptype = {'PAM': 'post-amp', 'FEM': 'front-end', 'SNP': 'snap'}
        cdt = cm_utils.get_astropytime(cdate, ctime)
        if not self.exists('part', hpn=old[0], rev=old[1], check_date=cdt):
            print("{} does not exist -- aborting swap".format(old[0]))
            return
        replace_with_none = False
        if new is None:
            replace_with_none = True
            print("Only stopping old part.")
        else:
            if not self.exists('part', hpn=new[0], rev=new[1], check_date=cdt):
                for ptk in ptype.keys():
                    if new[0].upper().startswith(ptk):
                        break
                new = new + [ptype[ptk], new[0]]
                print("Adding new part {}".format(new))
                self.update_part('add', new, cdate, ctime)
            else:
                print("{} already added.".format(new[0]))
        old_pd = self.handle.get_dossier(hpn=old[0], rev=old[1], at_date=cdt,
                                         active=self.active, exact_match=True)
        old_pd_key = list(old_pd.keys())
        if len(old_pd_key) > 1:
            print("Too many connected parts")
            return
        opd = old_pd[old_pd_key[0]]
        print("Stopping old connections: ")
        for key, val in opd.connections.up.items():
            uppart = [val.upstream_part, val.up_part_rev, val.upstream_output_port]
            dnpart = [val.downstream_part, val.down_part_rev, val.downstream_input_port]
            self.update_connection('stop', uppart, dnpart, cdate, ctime)
        for key, val in opd.connections.down.items():
            uppart = [val.upstream_part, val.up_part_rev, val.upstream_output_port]
            dnpart = [val.downstream_part, val.down_part_rev, val.downstream_input_port]
            self.update_connection('stop', uppart, dnpart, cdate, ctime)
        if replace_with_none:
            return
        print("Adding connections: ")
        for key, val in opd.connections.up.items():
            uppart = [val.upstream_part, val.up_part_rev, val.upstream_output_port]
            dnpart = [new[0], new[1], val.downstream_input_port]
            self.update_connection('add', uppart, dnpart, cdate, ctime)
        for key, val in opd.connections.down.items():
            uppart = [new[0], new[1], val.upstream_output_port]
            dnpart = [val.downstream_part, val.down_part_rev, val.downstream_input_port]
            self.update_connection('add', uppart, dnpart, cdate, ctime)

    def done(self):
        """Finish."""
        if self.bash_script_name is None:
            return
        self.fp.close()
        if self.verbose:
            print("----------------------DONE-----------------------")
        if not self.chmod:
            if self.verbose:
                print("If changes OK, 'chmod u+x {}' and run that script."
                      .format(self.bash_script_name))
        else:
            os.chmod(self.bash_script_name, 0o755)
            if self.verbose:
                print("Run {}".format(self.bash_script_name))
