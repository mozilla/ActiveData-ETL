# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from pyLibrary.structs import Dict


def key2etl(key):
    """
    CONVERT S3 KEY TO ETL HEADER

    S3 NAMING CONVENTION: a.b.c WHERE EACH IS A STEP IN THE ETL PROCESS
    HOW TO DEAL WITH a->b AS AGGREGATION?  (b:a).c?   b->c is agg: c:(a.b)
    NUMBER OF COMBINATIONS IS 2^n, SO PARENTHESIS MUST BE USED
    """
    if key.endswith(".json"):
        key = key[:-5]

    i = key.find(':')
    if i == -1:
        i = key.find('.')
    if i == -1:
        i = key.find('(')
    if i == -1:
        if key == 'None':
            return Dict(id=-1)
        return Dict(id=int(key))

    if key[i] == '(':
        e = key.rfind(')')
        subkey = key2etl(key[i + 1:e])

        i = key.find(':', start=e)
        if i == -1:
            i = key.find('.', start=e)
    else:
        subkey = Dict(id=int(key[:i]))

    if key[i] == ':':
        childkey = key2etl(key[i + 1:])
        subkey.source = childkey
        subkey.type = "aggregation"
        return subkey
    else:
        childkey = key2etl(key[i + 1:])
        childkey.source = subkey
        childkey.type = "join"
        return childkey


def etl2key(etl):
    if etl.source:
        if etl.source.type:
            if etl.type == etl.source.type:
                if etl.type == "join":
                    return etl2key(etl.source) + "." + unicode(etl.id)
                else:
                    return unicode(etl.id) + ":" + etl2key(etl.source)
            else:
                if etl.type == "join":
                    return "(" + etl2key(etl.source) + ")." + unicode(etl.id)
                else:
                    return unicode(etl.id) + ":(" + etl2key(etl.source) + ")"
        else:
            if etl.type == "join":
                return etl2key(etl.source) + "." + unicode(etl.id)
            else:
                return unicode(etl.id) + ":" + etl2key(etl.source)
    else:
        return unicode(etl.id)
