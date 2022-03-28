#!/bin/bash

# set up environment
source ~/.bashrc
conda activate HERA

# run scripts to write new hera_cm_db_updates snapshot
write_sqlite.py
cm_pack.py --go
cd /home/obs/src/hera_cm_db_updates
git push origin main
