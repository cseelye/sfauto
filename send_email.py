#!/usr/bin/python

# This script will send an email

# ----------------------------------------------------------------------------
# Configuration
#  These may also be set on the command line

smtp_server = None                          # The email server to use
                                            # --email_server

server_user = None                          # The username for the email server
                                            # --server_user

server_pass = None                          # The password for the email server
                                            # --server_pass

email_from = "testscript@nothing"           # The email address to send from
                                            # --email_from

email_to = [                                # The list of email addresses to send the mail to

]

email_subject = ""                          # The subject line for the email
                                            # --email_subject

email_body = ""                             # The body text for the email
                                            # --email_body

attachments = [                             # List of files to attach to the email
                                            # --attachments
]

# ----------------------------------------------------------------------------


import sys,os
from optparse import OptionParser
import libsf
from libsf import mylog


def main():
    # Pull in values from ENV if they are present
    if os.environ.get("SFEMAIL_NOTIFY"):
        globals()["email_to"] = os.environ["SFEMAIL_NOTIFY"]

    # Parse command line arguments
    parser = OptionParser()
    global smtp_server, server_user, server_pass, email_subject, email_body, email_from, email_to, attachments
    parser.add_option("--smtp_server", type="string", dest="smtp_server", default=smtp_server, help="the email server to use")
    parser.add_option("--server_user", type="string", dest="server_user", default=server_user, help="the username for the email server")
    parser.add_option("--server_pass", type="string", dest="server_pass", default=server_pass, help="the password for the email server")
    parser.add_option("--email_subject", type="string", dest="email_subject", default=email_subject, help="the subject line for the email")
    parser.add_option("--email_body", type="string", dest="email_body", default=email_body, help="the body text for the email")
    parser.add_option("--email_from", type="string", dest="email_from", default=email_from, help="the email address to send from")
    parser.add_option("--email_to", type="string", dest="email_to", default=email_to, help="the list of email addresses to send to")
    parser.add_option("--attachments", type="string", dest="attachments", default=attachments, help="the list of files to attach")
    parser.add_option("--debug", action="store_true", dest="debug", help="display more verbose messages")
    (options, args) = parser.parse_args()
    smtp_server = options.smtp_server
    server_user = options.server_user
    server_pass = options.server_pass
    email_subject = options.email_subject
    email_body = options.email_body
    email_from = options.email_from
    if (type(options.email_to) is list):
        email_to = options.email_to
    else:
        email_to = []
        pieces = str(options.email_to).split(",")
        for piece in pieces:
            piece = piece.strip()
            email_to.append(piece)

    if (type(options.attachments) is list):
        attachments = options.attachments
    else:
        attachments = []
        pieces = str(options.attachments).split(",")
        for piece in pieces:
            piece = piece.strip()
            attachments.append(piece)
    if options.debug != None:
        import logging
        mylog.console.setLevel(logging.DEBUG)

    try:
        libsf.SendEmail(email_to, email_subject, email_body, attachments, email_from, smtp_server, server_user, server_pass)
    except Exception, e:
        mylog.error("Error sending email: " + str(e))
        exit(1)


if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))
    try:
        timer = libsf.ScriptTimer()
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        exit(1)
    except:
        mylog.exception("Unhandled exception")
        exit(1)
    exit(0)





