# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division

_get = object.__getattribute__
_set = object.__setattr__


def inverse(d):
    """
    reverse the k:v pairs
    """
    output = {}
    for k, v in unwrap(d).iteritems():
        output[v] = output.get(v, [])
        output[v].append(k)
    return output


def nvl(*args):
    # pick the first none-null value
    for a in args:
        if a != None:
            return wrap(a)
    return Null


def zip(keys, values):
    """
    CONVERT LIST OF KEY/VALUE PAIRS TO A DICT
    """
    output = Dict()
    for i, k in enumerate(keys):
        output[k] = values[i]
    return output



def literal_field(field):
    """
    RETURN SAME WITH . ESCAPED
    """
    try:
        return field.replace(".", "\.")
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.error("bad literal", e)

def split_field(field):
    """
    RETURN field AS ARRAY OF DOT-SEPARATED FIELDS
    """
    if field.find(".") >= 0:
        field = field.replace("\.", "\a")
        return [k.replace("\a", ".") for k in field.split(".")]
    else:
        return [field]


def join_field(field):
    """
    RETURN field SEQUENCE AS STRING
    """
    return ".".join([f.replace(".", "\.") for f in field])


def hash_value(v):
    if isinstance(v, (set, tuple, list)):
        return hash(tuple(hash_value(vv) for vv in v))
    elif not isinstance(v, dict):
        return hash(v)
    else:
        return hash(tuple(sorted(hash_value(vv) for vv in v.values())))



def _setdefault(obj, key, value):
    """
    DO NOT USE __dict__.setdefault(obj, key, value), IT DOES NOT CHECK FOR obj[key] == None
    """
    v = obj.get(key, None)
    if v == None:
        obj[key] = value
        return value
    return v


def set_default(*params):
    """
    INPUT dicts IN PRIORITY ORDER
    UPDATES FIRST dict WITH THE MERGE RESULT, WHERE MERGE RESULT IS DEFINED AS:
    FOR EACH LEAF, RETURN THE HIGHEST PRIORITY LEAF VALUE
    """
    agg = params[0] if params[0] != None else {}
    for p in params[1:]:
        p = unwrap(p)
        if p is None:
            continue
        _all_default(agg, p)
    return wrap(agg)


def _all_default(d, default):
    """
    ANY VALUE NOT SET WILL BE SET BY THE default
    THIS IS RECURSIVE
    """
    if default is None:
        return
    for k, default_value in default.items():
        existing_value = d.get(k, None)
        if existing_value is None:
            d[k] = default_value
        elif isinstance(existing_value, dict) and isinstance(default_value, dict):
            _all_default(existing_value, default_value)


def _getdefault(obj, key):
    """
    TRY BOTH ATTRIBUTE AND ITEM ACCESS, OR RETURN Null
    """
    try:
        return obj.__getattribute__(key)
    except Exception, e:
        pass

    try:
        return obj[key]
    except Exception, f:
        pass

    try:
        if float(key) == round(float(key), 0):
            return eval("obj["+key+"]")
    except Exception, f:
        pass

    try:
        return eval("obj."+unicode(key))
    except Exception, f:
        pass

    return NullType(obj, key)





from pyLibrary.structs.nones import Null, NullType
from pyLibrary.structs.dicts import Dict
from pyLibrary.structs.wraps import wrap, unwrap
