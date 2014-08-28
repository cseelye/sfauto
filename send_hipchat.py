#!/usr/bin/python

"""
This action will send a message to a hipchat channel

When run as a script, the following options/env variables apply:
    --room_id           The room ID to message

    --user_name         The user name to post as

    --color             The color to highlight the message

    --message           The message to send

"""

import json
import sys
import urllib2

from optparse import OptionParser
import lib.libsf as libsf
from lib.libsf import mylog
import lib.sfdefaults as sfdefaults
from lib.action_base import ActionBase

class SendHipchatAction(ActionBase):
    class Events:
        """
        Events that this action defines
        """

    def __init__(self):
        super(self.__class__, self).__init__(self.__class__.Events)

    def ValidateArgs(self, args):
        libsf.ValidateArgs({"room_id" : libsf.IsInteger,
                            "message" : None,
                            "user_name" : None,
                            "color" : None
                            },
            args)
        valid_colors = ["yellow", "red", "green", "purple", "gray", "random"]
        if args["color"] not in valid_colors:
            raise libsf.SfArgumentError("Invalid color - please use one of " + str(valid_colors))

    def Execute(self, room_id, message, user_name, color, debug=False):
        """
        Send an email
        """
        self.ValidateArgs(locals())

        message = message.replace(" ", "+")
        user_name = user_name.replace(" ", "+")
        try:
            # Very quick and dirty - let's come back and make this better some day
            url = "https://api.hipchat.com/v1/rooms/message?auth_token=8f703f96c365afce41b84db3f69f34&room_id={}&from={}&message={}&color={}&message_format=text".format(room_id, user_name, message, color)
            request = urllib2.Request(url)
            response = urllib2.urlopen(request)
            response_str = response.read().decode('ascii')
        except Exception as e:
            mylog.error(str(e))
            return False

        try:
            response_obj = json.loads(response_str)
        except ValueError:
            mylog.error("Invalid JSON reply")
            return False

        if "error" in response_obj:
            mylog.error("Something went wrong - " + response_obj["error"]["message"])
            return False

        return True

# Instantate the class and add its attributes to the module
# This allows it to be executed simply as module_name.Execute
libsf.PopulateActionModule(sys.modules[__name__])

if __name__ == '__main__':
    mylog.debug("Starting " + str(sys.argv))

    # Parse command line arguments
    parser = OptionParser(option_class=libsf.ListOption, description=libsf.GetFirstLine(sys.modules[__name__].__doc__))
    parser.add_option("--room_id", type="int", dest="room_id", default=0, help="the room ID to send the message to")
    parser.add_option("--message", type="string", dest="message", default=None, help="the message to send")
    parser.add_option("--user_name", type="string", dest="user_name", default=sfdefaults.hipchat_user, help="the username to post as [%default]")
    parser.add_option("--color", type="string", dest="color", default=sfdefaults.hipchat_color, help="the color of the message [%default]")
    parser.add_option("--debug", action="store_true", dest="debug", default=False, help="display more verbose messages")
    (options, extra_args) = parser.parse_args()

    try:
        timer = libsf.ScriptTimer()
        if Execute(room_id=options.room_id, message=options.message, user_name=options.user_name, color=options.color, debug=options.debug):
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
