#! /usr/bin/env python
# -*- mode: python; coding: utf-8 -*-
# Copyright 2020 the HERA Collaboration
# Licensed under the 2-clause BSD license.

"""System watch-dogs."""


def read_forward_list():  # pragma: no cover
    """Read in emails from .forward file."""
    fwd = []
    with open("~/.forward", "r") as fp:
        for line in fp:
            fwd.append(line.strip())


def send_email(
    subject, msg, to_addr=None, from_addr="", skip_send=False
):
    """
    Send an email message, unless skip_send is True (for testing).

    Parameters
    ----------
    subject : str
        Subject of email.
    msg : str
        Message to send.
    to_addr : list or None
        If None, it will read the list in the .forward file.
    from_addr : str
        From address to use
    skip_send : bool
        If True, it will just return the composed message and not send it.

    Returns
    -------
    EmailMessage

    """
    import smtplib
    from email.message import EmailMessage

    if to_addr is None:  # pragma: no cover
        to_addr = read_forward_list()
    email = EmailMessage()
    email.set_content(msg)
    email["From"] = from_addr
    email["To"] = to_addr
    email["Subject"] = subject
    if not skip_send:  # pragma: no cover
        server = smtplib.SMTP("localhost")
        server.send_message(email)
        server.quit()
    return email


