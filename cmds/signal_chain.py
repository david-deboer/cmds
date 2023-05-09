"""Various signal chain modification methods."""
from . import cm, cm_utils, cm_active, cm_handling, upd_util
import os

class Update:
    """Holds the various update methods."""

    snap_ports = [{'e': 'e2', 'n': 'n0'}, {'e': 'e6', 'n': 'n4'}, {'e': 'e10', 'n': 'n8'}]

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
        for key, val in self.part_types.items():
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


