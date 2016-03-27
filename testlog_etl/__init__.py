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
from pyLibrary.aws import s3
from pyLibrary.collections import reverse
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap
from pyLibrary.queries import qb


def key2etl(key):
    """
    CONVERT S3 KEY TO ETL HEADER

    S3 NAMING CONVENTION: a.b.c WHERE EACH IS A STEP IN THE ETL PROCESS
    HOW TO DEAL WITH a->b AS AGGREGATION?  b:a.c?   b->c is agg: a.c:b
    """
    key = s3.strip_extension(key)

    tokens = []
    s = 0
    i = strings.find(key, [":", "."])
    while i < len(key):
        tokens.append(key[s:i])
        tokens.append(key[i])
        s = i + 1
        i = strings.find(key, [":", "."], s)
    tokens.append(key[s:i])

    _reverse_aggs(tokens)
    # tokens.reverse()
    source = {
        "id": format_id(tokens[0])
    }
    for i in range(2, len(tokens), 2):
        source = {
            "id": format_id(tokens[i]),
            "source": source,
            "type": "join" if tokens[i - 1] == "." else "agg"
        }
    return wrap(source)


def format_id(value):
    """
    :param value:
    :return: int() IF POSSIBLE
    """
    try:
        return int(value)
    except Exception:
        return unicode(value)


def _parse_key(elements):
    """
    EXPECTING ALTERNATING LIST OF operands AND operators
    """
    if isinstance(elements, basestring):
        try:
            return {"id": format_id(elements)}
        except Exception, e:
            Log.error("problem", e)
    if isinstance(elements, list) and len(elements) == 1:
        if isinstance(elements[0], basestring):
            return {"id": format_id(elements[0])}
        return elements[0]
    if isinstance(elements, Mapping):
        return elements

    for i in reversed(range(1, len(elements), 2)):
        if elements[i] == ":":
            return _parse_key(elements[:i - 1:] + [{"id": format_id(elements[i - 1]), "source": _parse_key(elements[i + 1]), "type": "agg"}] + elements[i + 2::])
    for i in range(1, len(elements), 2):
        if elements[i] == ".":
            return _parse_key(elements[:i - 1:] + [{"id": format_id(elements[i + 1]), "source": _parse_key(elements[i - 1]), "type": "join"}] + elements[i + 2::])
    Log.error("Do not know how to parse")


def etl2key(etl):
    source = etl
    seq = []
    while source:
        seq.append(unicode(format_id(source.id)))
        if source.type == "join":
            seq.append(".")
        else:
            seq.append(":")
        source = source.source
    seq = seq[:-1]

    _reverse_aggs(seq)
    seq.reverse()

    return "".join(seq)


def _reverse_aggs(seq):
    # SHOW AGGREGATION IN REVERSE ORDER (ASSUME ONLY ONE)
    for i in range(1, len(seq), 2):
        if seq[i] == ":":
            seq[i - 1], seq[i + 1] = seq[i + 1], seq[i - 1]


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
