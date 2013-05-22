#!/usr/bin/python

"""
This action will remove an IQN from a volume access group

When run as a script, the following options/env variables apply:
    --email_to          List of addresses to send email to

    --email_subject     Subject line for the email

    --email_body        Body of the email

    --attachements      List of filenames to attach to the email

    --email_from        Email address to send the email form

    --smtp_server       Email server to use

    --smtp_user         Username for the email server

    --smtp_pass         Password for the email server
"""

import sys
from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import logging
import inspect
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase
from lib.datastore import SharedValues

class SendEmailAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """
        BEFORE_EMAIL = "BEFORE_EMAIL"
        AFTER_EMAIL = "AFTER_EMAIL"
        EMAIL_ERROR = "EMAIL_ERROR"

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"emailTo" : None,
                            "emailSubject" : None
                            },
            args)

    def Execute(self, emailTo, emailSubject, emailBody=None, attachments=None, emailFrom=sfdefaults.email_from, SMTPServer=sfdefaults.smtp_server, SMTPUser=None, SMTPPass=None, debug=False):
        """
        Send an email
        """
        self.ValidateArgs(locals())
        if debug:
            mylog.console.setLevel(logging.DEBUG)

        self._RaiseEvent(self.Events.BEFORE_EMAIL)
        try:
            libsf.SendEmail(emailTo, emailSubject, emailBody, attachments, emailFrom, SMTPServer, SMTPUser, SMTPPass)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            mylog.error("Error sending email: " + str(e))
            self._RaiseEvent(self.Events.EMAIL_ERROR)
            return False

        self._RaiseEvent(self.Events.AFTER_EMAIL)
        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--smtp_server", type="string", dest="smtp_server", default=sfdefaults.smtp_server, help="the email server to use [%default]")
    parser.add_option("--server_user", type="string", dest="server_user", default=None, help="the username for the email server [%default]")
    parser.add_option("--server_pass", type="string", dest="server_pass", default=None, help="the password for the email server [%default]")
    parser.add_option("--email_subject", type="string", dest="email_subject", default=None, help="the subject line for the email")
    parser.add_option("--email_body", type="string", dest="email_body", default=None, help="the body text for the email")
    parser.add_option("--email_from", type="string", dest="email_from", default=sfdefaults.email_from, help="the email address to send from [%default]")
    parser.add_option("--email_to", action="list", dest="email_to", default=None, help="the list of email addresses to send to")
    parser.add_option("--attachments", action="list", dest="attachments", default=None, help="the list of files to attach")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(options.email_to, options.email_subject, options.email_body, options.attachments, options.email_from, options.smtp_server, options.server_user, options.server_pass, options.debug):
            sys.exit(0)
        else:
            sys.exit(1)
    except libsf.SfArgumentError as e:
        mylog.error("Invalid arguments - \n" + str(e))
        sys.exit(1)
    except SystemExit:
        raise
    except KeyboardInterrupt:
        mylog.warning("Aborted by user")
        Abort()
        sys.exit(1)
    except:
        mylog.exception("Unhandled exception")
        sys.exit(1)

