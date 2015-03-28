# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import unicode_literals
from __future__ import division

from pyLibrary.collections.matrix import Matrix
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import listwrap, unwrap
from pyLibrary.queries import windows
from pyLibrary.queries.cube import Cube
from pyLibrary.queries.domains import SimpleSetDomain, is_keyword
# from pyLibrary.queries.py.util import util_filter
from pyLibrary.queries.py.expressions import qb_expression_to_function


def is_py_aggs(query):
    if query.edges or query.groupby or any(a != None and a != "none" for a in listwrap(query.select).aggregate):
        return True
    return False


def py_aggs(frum, query):
    select = listwrap(query.select)

    for e in query.edges:
        if not is_keyword(e.value):
            Log.error("not implemented yet")

        e.domain = SimpleSetDomain(partitions=list(sorted(set(frum.select(e.value)))))

    result = {s.name: Matrix(dims=[len(e.domain.partitions) for e in query.edges], zeros=s.aggregate == "count") for s in select}
    where = qb_expression_to_function(query.where)
    for d in filter(where, frum):
        coord = tuple(e.domain.getIndexByKey(d[e.value]) for e in query.edges)
        for s in select:
            if s.aggregate == "count":
                if s.value == "." or s.value == None:
                    result[s.name][coord] += 1
                elif d[s.value] != None:
                    result[s.name][coord] += 1
            else:
                acc = result[s.name][coord]
                if acc == None:
                    acc = windows.name2accumulator.get(s.aggregate)
                    if acc ==None:
                        Log.error("select aggregate {{agg}} is not recognized", {"agg":s.aggregate})
                    acc = acc(**unwrap(s))
                    result[s.name][coord] = acc
                acc.add(d[s.value])

    for s in select:
        if s.aggregate == "count":
            continue
        m = result[s.name]
        for c, v in m.items():
            if v != None:
                m[c] = v.end()

    return Cube(select, query.edges, result)
