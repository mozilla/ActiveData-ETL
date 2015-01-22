# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#


import os
from pyLibrary.debugs.logs import Log

from pyLibrary.env.files import File
from pyLibrary.dot import set_default, wrap
from pyLibrary import convert


def get(url):
    """
    USE son.net CONVENTIONS TO LINK TO INLINE OTHER JSON
    """
    if not url.find("://"):
        Log.error("{{url}} must have a prototcol (eg http://) declared", {"url": url})
    doc = wrap({"$ref": url})
    return _replace_ref(doc, "", [doc])  # BLANK URL ONLY WORKS IF url IS ABSOLUTE


def expand(doc, doc_url):
    """
    ASSUMING YOU ALREADY PULED THE doc FROM doc_url, YOU CAN STILL USE THE
    EXPANDING FEATURE
    """
    if not doc_url.find("://"):
        Log.error("{{url}} must have a prototcol (eg http://) declared", {"url": doc_url})
    return _replace_ref(doc, "", [doc])  # BLANK URL ONLY WORKS IF url IS ABSOLUTE


def _replace_ref(node, url, doc_path):
    if url.endswith("/"):
        url = url[:-1]

    if isinstance(node, dict):
        ref, node["$ref"] = node["$ref"], None

        if not ref:
            # RECURS
            return_value = node
            candidate = {}
            for k, v in node.items():
                new_v = _replace_ref(v, url + "/" + convert.value2url(k), [v] + doc_path)
                candidate[k] = new_v
                if new_v is not v:
                    return_value = candidate
            return return_value

        if ref.startswith("//"):
            # SCHEME RELATIVE IMPLIES SAME PROTOCOL AS LAST TIME, WHICH
            # REQUIRES THE CURRENT DOCUMENT'S SCHEME
            ref = url.split("://")[0] + ":" + ref

        if ref.startswith("http://"):
            new_value = convert.json2value(http.get(ref), flexible=True, paths=True)
        elif ref.startswith("file://"):
            ref = ref[7::]
            if ref.startswith("/"):
                new_value = File(ref).read_json(ref)
            else:
                new_value = File.new_instance(url[7::], ref).read_json()
        elif ref.startswith("env://"):
            # GET ENVIRONMENT VARIABLES
            ref = ref[6::]
            try:
                new_value = convert.json2value(os.environ[ref])
            except Exception, e:
                new_value = os.environ[ref]
        elif ref.find("://"):
            raise Log.error("unknown protocol {{scheme}}", {"scheme": ref.split("://")[0]})
        else:
            # REFER TO SELF
            if ref[0] == ".":
                # RELATIVE
                for i, p in enumerate(ref):
                    if p != ".":
                        new_value = doc_path[i][ref[i::]]
                        break
                else:
                    new_value = doc_path[len(ref) - 1]
            else:
                # ABSOLUTE
                new_value = doc_path[-1][ref]

        if node:
            return set_default({}, node, new_value)
        else:
            return new_value

    elif isinstance(node, list):
        candidate = [_replace_ref(n, url, [n] + doc_path) for n in node]
        if all(p[0] is p[1] for p in zip(candidate, node)):
            return node
        return candidate

    return node
