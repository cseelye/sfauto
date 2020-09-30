#!/usr/bin/env python
#pylint: disable=protected-access

import random
import string

def RandomString(length):
    return "".join(random.choice(string.ascii_letters + string.digits) for i in range(length))

def RandomIQN():
    return "iqn.{}.{}.{}.{}:{}".format("".join(random.choice(string.digits) for i in range(4)),
                                       RandomString(3),
                                       RandomString(8),
                                       RandomString(3),
                                       RandomString(12)).lower()

def RandomWWN():
    num = str(random.randint(0, 9999))
    return "50:00:0c:89:35:36:d{}:{}".format(num[0], num[1:])

def RandomIP():
    return "{}.{}.{}.{}".format(random.randint(1, 254),
                                random.randint(1, 254),
                                random.randint(1, 254),
                                random.randint(1, 254))

def RandomSequence(length, distinctFrom=None):
    distinct_from = distinctFrom or []
    seq = []
    for _ in range(length):
        while True:
            el = random.randint(1, length * 1000)
            if el not in seq and el not in distinct_from:
                seq.append(el)
                break
    return seq

