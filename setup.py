#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2018 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""Module setup."""

import glob
import io
import sys
from setuptools import setup

# add hera_mc to our path in order to use the branch_scheme function
sys.path.append("cmds")
from branch_scheme import branch_scheme  # noqa

with io.open("README.md", "r", encoding="utf-8") as readme_file:
    readme = readme_file.read()

setup_args = {
    "name": "cmds",
    "description": "cmds: Configuration Management Data System",
    "long_description": readme,
    "url": "https://github.com/david-deboer/cmds",
    "license": "BSD",
    "author": "David R. DeBoer",
    "author_email": "ddeboer@berkeley.edu",
    "use_scm_version": {"local_scheme": branch_scheme},
    "packages": ["cmds"],
    "scripts": glob.glob("scripts/*"),
    "include_package_data": True,
    "install_requires": [
        "alembic",
        "astropy",
        "cartopy",
        "numpy",
        "psycopg2",
        "pyyaml",
        "redis",
        "setuptools_scm",
        "sqlalchemy",
    ],
    "extras_require": {
        "sqlite": ["tabulate"],
        "all": ["h5py", "pandas", "psutil", "python-dateutil", "tabulate", "tornado"],
        "dev": [
            "h5py",
            "pandas",
            "psutil",
            "python-dateutil",
            "tabulate",
            "tornado",
            "pytest",
            "pre-commit",
        ],
    },
    "tests_require": ["pyyaml"],
    "classifiers": [
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
    ],
}

if __name__ == "__main__":
    setup(**setup_args)
