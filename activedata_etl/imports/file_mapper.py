# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (klahnakoski@mozilla.com)

from __future__ import division
from __future__ import unicode_literals

import re

from activedata_etl.imports.coverage_util import download_file
from activedata_etl.transforms import ACTIVE_DATA_QUERY
from jx_base.expressions import last
from jx_python.expressions import jx_expression_to_function
from mo_dots import coalesce
from mo_files import TempFile
from mo_future import text
from mo_json import stream
from mo_logs import Log
from mo_times import Timer, Date, Duration
from pyLibrary.env import http
from pyLibrary.env.big_data import scompressed2ibytes


class FileMapper(object):
    """
    MAP FROM COVERAGE FILE RESOURCE NAME TO SOURCE FILENAME
    """

    def __init__(self, source_key, task_cluster_record):
        """
        :param task_cluster_record: EXPECTING TC RECORD WITH repo.push.date SO AN APPROXIMATE SOURCE FILE LIST CAN BE FOUND
        """

        # TODO: THERE IS A RISK THE FILE MAPPING MAY CHANGE
        # FIND RECENT FILE LISTING
        timestamp = Date(
            coalesce(
                task_cluster_record.repo.push.date,
                task_cluster_record.repo.changeset.date,
                Date.now(),
            )
        ) - Duration("hour")
        result = http.post_json(
            ACTIVE_DATA_QUERY,
            json={
                "from": "task.task.artifacts",
                "where": {
                    "and": [
                        {"eq": {"name": "public/components.json.gz"}},
                        {"eq": {"treeherder.symbol": "Bugzilla"}},
                        {"lt": {"repo.push.date": timestamp}},
                    ]
                },
                "sort": {"repo.push.date": "desc"},
                "limit": 100,
                "select": ["url", "repo.push.date"],
                "format": "list",
            },
        )

        self.predefined_failures = jx_expression_to_function(KNOWN_FAILURES)
        # REPLACE THIS WITH predefined failures, once dev has been merged
        self.complicated_failures = (
            lambda filename: "cargo/registry/src/github.com" in filename
        )
        self.known_failures = set()
        self.lookup = {}
        for files_url in result.data.url:
            try:
                with TempFile() as tempfile:
                    Log.note("download {{url}}", url=files_url)
                    download_file(files_url, tempfile.abspath)
                    with open(tempfile.abspath, str("rb")) as fstream:
                        with Timer("process {{url}}", param={"url": files_url}):
                            count = 0
                            for data in stream.parse(
                                scompressed2ibytes(fstream), {"items": "."}, {"name"}
                            ):
                                self._add(data.name)
                                count += 1
                            Log.note(
                                "{{count}} files in {{file}}",
                                count=count,
                                file=files_url,
                            )
                return
            except Exception as e:
                Log.note(
                    "Can not read {{url}} for key {{key}}",
                    url=files_url,
                    key=source_key,
                )
        else:
            Log.error(
                "Can not read FileMapper {{url}} (and {{others}} others) for key {{key}}",
                url=last(result.data).url,
                others=len(result.data.url) - 1,
                key=source_key,
                cause=e,
            )

    def _add(self, filename):
        if filename.startswith(EXCLUDE):
            return

        path = list(reversed(filename.split("/")))
        curr = self.lookup
        for i, p in enumerate(path):
            found = curr.get(p)
            if not found:
                curr[p] = filename
                return
            elif isinstance(found, text):
                if i + 1 >= len(path):
                    curr[p] = {".": filename}
                else:
                    curr[p] = {path[i + 1]: filename}
                self._add(found)
                return
            else:
                curr = found

    def find(self, source_key, filename, artifact, task_cluster_record):
        """
        :param source_key: FOR DEBUGGING
        :param filename: THE FILE TO LOOK FOR
        :param artifact: THE ARTIFACT (FOR DEBUGGING)
        :param task_cluster_record: FOR OTHER INFO THAT MAY HELP IDENTIFYING THE RIGHT SOURCE FILE
        :return: {"name":name, "old_name":old_name, "is_firefox":boolean}
        """

        def find_best(files, complain):
            filename_words = (
                set(n for n in re.split(r"\W", filename) if n) | suite_names
            )
            best = None
            best_score = 0
            peer = None
            for f in files:
                f_words = set(n for n in re.split("\W", f) if n)
                score = len(filename_words & f_words) / len(filename_words | f_words)
                if score > best_score:
                    best = f
                    peer = None
                    best_score = score
                elif score == best_score:
                    peer = f
            if best and not peer:
                if best == filename:
                    return {"name": best, "is_firefox": True}
                else:
                    return {"name": best, "is_firefox": True, "old_name": filename}
            else:
                self.known_failures.add(filename)
                if complain:
                    Log.warning(
                        "Can not resolve {{filename}} in {{url}} for key {{key}}. Too many candidates: {{list|json}}",
                        key=source_key,
                        url=artifact.url,
                        filename=filename,
                        list=files,
                    )
                return {"name": filename}

        try:
            found = KNOWN_MAPPINGS.get(filename)
            if found:
                return {"name": found, "is_firefox": True, "old_name": filename}
            if self.complicated_failures(filename):
                return {"name": filename}
            if self.predefined_failures(filename):
                return {"name": filename}
            if filename in self.known_failures:
                return {"name": filename}
            suite_names = SUITES.get(
                task_cluster_record.suite.name, {task_cluster_record.run.suite.name}
            )

            filename = (
                filename.split(" line ")[0]
                .split(" -> ")[-1]
                .split("?")[0]
                .split("#")[0]
            )  # FOR URLS WITH PARAMETERS
            path = list(reversed(filename.split("/")))
            curr = self.lookup
            i = -1
            for i, p in enumerate(path):
                if p == ".":
                    continue
                found = curr.get(p)
                if not found:
                    break
                elif isinstance(found, text):
                    if found == filename:
                        return {"name": found, "is_firefox": True}
                    else:
                        return {"name": found, "is_firefox": True, "old_name": filename}
                else:
                    curr = found

            if i == 0:  # WE MATCH NOTHING, DO NOT EVEN TRY FOR A BETTER MATCH
                return {"name": filename}
            return find_best(list(sorted(_values(curr))), i > 1)
        except Exception as ee:
            Log.warning(
                "Can not resolve {{filename}} in {{url}} for key {{key}}",
                key=source_key,
                url=artifact.url,
                filename=filename,
                cause=ee,
            )


def _values(curr):
    for v in curr.values():
        if isinstance(v, text):
            yield v
        else:
            for u in _values(v):
                yield u


KNOWN_FAILURES = {
    "or": [
        {
            "in": {
                ".": [
                    "chrome://damp/content/framescript.js",
                    "chrome://global/content/bindings/tree.xml",
                    "chrome://mochitests/content/browser/devtools/client/netmonitor/test/shared-head.js",
                    "chrome://mochitests/content/browser/devtools/shared/worker/tests/browser/head.js",
                    "chrome://pageloader/content/utils.js",
                    "chrome://pageloader/content/Profiler.js",
                    "chrome://workerbootstrap/content/worker.js",
                    "decorators.py",
                    "http://example.com/tests/SimpleTest/SimpleTest.js",
                    "http://mochi.test:8888/resources/testharnessreport.js",
                    "http://mochi.test:8888/tests/SimpleTest/SimpleTest.js",
                    "http://mochi.test:8888/tests/SimpleTest/TestRunner.js",
                    "http://web-platform.test:8000/dom/common.js",
                    "http://web-platform.test:8000/dom/historical.html",
                    "http://web-platform.test:8000/dom/interfaces.html",
                    "http://web-platform.test:8000/testharness_runner.html",
                    "https://example.com/tests/SimpleTest/SimpleTest.js",
                    "https://example.com/tests/SimpleTest/TestRunner.js",
                    "resource://gre/modules/workers/require.js",
                    "resource://services-common/utils.js",
                    "resource://services-crypto/utils.js",
                    "numerics/safe_conversions_impl.h",
                    "decode.h",
                ]
            }
        },
        {"suffix": {".": "/mod.rs"}},
        {"suffix": {".": "/actions/index.js"}},
        {"suffix": {".": "/components/App.js"}},
        {"suffix": {".": "/reducers/index.js"}},
        {"suffix": {".": "/error.rs"}},
        {"suffix": {".": "/build/tests/xpcshell/head.js"}},
        {"suffix": {".": "/shared/tests/browser/head.js"}},
        {"suffix": {".": "mozilla.org.xpi!/bootstrap.js"}},
        {"suffix": {".": "/src/test.rs"}},
        # {"suffix": {".": "/ui.js"}},
        {"suffix": {".": "/utils/utils.js"}},
        {"suffix": {".": "/safe_conversions_impl.h"}},
        {"suffix": {".": "/safe_conversions.h"}},
        {"suffix": {".": "/src/result.rs"}},
        {"suffix": {".": "/./decode.h"}},
        {"prefix": {".": "data:"}},
        {"prefix": {".": "javascript:"}},
        {"prefix": {".": "about:"}},
        {"prefix": {".": "http://mochi.test:8888/MochiKit/"}},
        {"prefix": {".": "https://example.com/MochiKit"}},
        {"prefix": {".": "vs2017"}},
        {"prefix": {".": "third_party/rust"}},
        {"prefix": {".": "third_party/dav1d"}},
        {"prefix": {".": "devtools/client/"}},
    ]
}
KNOWN_MAPPINGS = {
    "http://example.org/tests/SimpleTest/TestRunner.js": "dom/tests/mochitest/ajax/mochikit/tests/SimpleTest/TestRunner.js"
}
EXCLUDE = ("mobile",)  # TUPLE OF SOURCE DIRECTORIES TO EXCLUDE
SUITES = {  # SOME SUITES ARE RELATED TO A NUMBER OF OTHER NAMES, WHICH CAN IMPROVE SCORING
    "web-platform-tests": {"web", "platform", "tests", "test", "wpt"},
    "mochitest": {"mochitest", "mochitests"},
}
