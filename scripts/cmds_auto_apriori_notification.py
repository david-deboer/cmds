#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2019 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Script to run the info update between the googlesheet and database."""

from hera_cm import upd_info

update = upd_info.UpdateInfo()
update.process_apriori_notification()
update.finish()
