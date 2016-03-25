# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals

from collections import Mapping

from pyLibrary import strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.maths import Math
from pyLibrary.queries import qb


def key2etl(key):
    """
    CONVERT S3 KEY TO ETL HEADER

    S3 NAMING CONVENTION: a.b.c WHERE EACH IS A STEP IN THE ETL PROCESS
    HOW TO DEAL WITH a->b AS AGGREGATION?  (b:a).c?   b->c is agg: c:(a.b)
    NUMBER OF COMBINATIONS IS 2^n, SO PARENTHESIS MUST BE USED

    SPECIAL CASE b:a.c.d WAS MEANT TO BE (b:a).c.d, BUT THERE WAS A BUG

    """
    if key.endswith(".json"):
        key = key[:-5]

    tokens = []
    s = 0
    i = strings.find(key, [":", "."])
    while i < len(key):
        tokens.append(key[s:i])
        tokens.append(key[i])
        s = i + 1
        i = strings.find(key, [":", "."], s)
    tokens.append(key[s:i])
    return wrap(_parse_key(tokens))


def _parse_key(elements):
    """
    EXPECTING ALTERNATING LIST OF operands AND operators
    """
    if isinstance(elements, basestring):
        try:
            return {"id": int(elements)}
        except Exception, e:
            Log.error("problem", e)
    if isinstance(elements, list) and len(elements) == 1:
        if isinstance(elements[0], basestring):
            return {"id": int(elements[0])}
        return elements[0]
    if isinstance(elements, Mapping):
        return elements

    for i in reversed(range(1, len(elements), 2)):
        if elements[i] == ":":
            return _parse_key(elements[:i - 1:] + [{"id": int(elements[i - 1]), "source": _parse_key(elements[i + 1]), "type": "aggregation"}] + elements[i + 2::])
    for i in range(1, len(elements), 2):
        if elements[i] == ".":
            return _parse_key(elements[:i - 1:] + [{"id": int(elements[i + 1]), "source": _parse_key(elements[i - 1]), "type": "join"}] + elements[i + 2::])
    Log.error("Do not know how to parse")


def etl2key(etl, source_name):
    seq = []
    while etl:
        seq.append(unicode(int(etl.id)))
        if etl.join == "join":
            seq.append(".")
        else:
            seq.append(":")
        etl = etl.source
    seq = seq[1:]


    # SHOW AGGREGATION IN REVERSE ORDER (ASSUME ONLY ONE)
    for i in range(1, len(seq), 2):
        if seq[i] == ":":
            seq[i - 1], seq[i + 1] = seq[i + 1], seq[i - 1]

    return "".join(reversed(seq))


def etl2path(etl):
    """
    CONVERT ETL TO A KEY PREFIX PATH
    """
    try:
        path = []
        while etl:
            path.append(etl.id)
            while etl.type and etl.type != "join":
                etl = etl.source
            etl = etl.source
        return qb.reverse(path)
    except Exception, e:
        Log.error("Can not get path {{etl}}",  etl= etl, cause=e)

def key2path(key):
    return etl2path(key2etl(key))

from . import transforms
