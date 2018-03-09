# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Trung Do (chin.bimbo@gmail.com)
#
from __future__ import division
from __future__ import unicode_literals

from future.utils import text_type

from activedata_etl.imports.buildbot import BUILD_TYPES
from activedata_etl.transforms.perfherder_logs_to_perf_logs import KNOWN_PERFHERDER_TESTS
from mo_dots import Data, coalesce, set_default
from mo_hg.hg_mozilla_org import minimize_repo
from mo_logs import strings, Log


def minimize_task(task):
    """
    task objects are a little large, scrub them of some of the
    nested arrays
    :param task: task cluster normalized object
    :return: altered object
    """
    task.repo = minimize_repo(task.repo)

    task.action.timings = None
    task.action.etl = None
    task.build.build = None
    task.build.task = {"id": task.build.task.id}
    task.etl = None
    task.task.artifacts = None
    task.task.created = None
    task.task.command = None
    task.task.env = None
    task.task.expires = None
    task.task.retries = None
    task.task.routes = None
    task.task.run = None
    task.task.runs = None
    task.task.scopes = None
    task.task.tags = None
    task.task.signing = None
    task.task.features = None
    task.task.image = None
    task.worker = {"aws": task.worker.aws}


def decode_metatdata_name(name):
    if name.startswith(NULL_TASKS):
        return {}

    for category, patterns in COMPILED_CATEGORIES.items():
        if name.startswith(category):
            for p, v in patterns:
                result = p.match(name[len(category):])
                if result != None:
                    return set_default(result, v)
            else:
                Log.warning("{{name|quote}} can not be processed with {{category}}", name=name, category=category)
                break
    return {}


class Matcher(object):

    def __init__(self, pattern):
        if pattern.startswith("{{"):
            var_name = strings.between(pattern, "{{", "}}")
            self.pattern = globals()[var_name]
            self.literal = None
            remainder = pattern[len(var_name)+4:]
        else:
            self.pattern = None
            self.literal = coalesce(strings.between(pattern, None, "{{"), pattern)
            remainder = pattern[len(self.literal):]

        if remainder:
            self.child = Matcher(remainder)
        else:
            self.child = Data(match=lambda name: None if name else {})

    def match(self, name):
        if self.pattern:
            for k, v in self.pattern.items():
                if name.startswith(k):
                    match = self.child.match(name[len(k):])
                    if match != None:
                        return set_default(match, v)
        elif self.literal:
            if name.startswith(self.literal):
                return self.child.match(name[len(self.literal):])
        return None


NULL_TASKS = (
    "Buildbot/mozharness S3 uploader",
    "balrog-",
    "beetmover-",
    "build-signing-",
    "checksums-signing-",
    "Cron task for ",
    "partials-signing-",
    "partials-",
    "repackage-l10n-",
    "nightly-l10n-"
)

CATEGORIES = {
    "source-test-": {
        "mozlint-codespell": {},
        "mozlint-cpp-virtual-final": {},
        "mozlint-test-manifest": {},
        "mozlint-eslint": {},
        "file-metadata-bugzilla-components": {},
        "mozlint-py-compat": {},
        "mozlint-shellcheck": {},
        "mozlint-py-flake8": {},
        "mozlint-wptlint-gecko": {}
    },
    "test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"build": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"build": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"build": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}": {"action": {"type": "test"}}
    },
    "build-": {
        "{{BUILD_PLATFORM}}/{{BUILD_TYPE}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}/{{BUILD_TYPE}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}-nightly/{{BUILD_TYPE}}": {"build": {"trigger": "nightly"}, "action": {"type": "build"}}
    },
    "desktop-test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
    }
}

BUILD_TYPE = {
    "opt": {"build": {"type": ["opt"]}},
    "debug": {"build": {"type": ["opt"]}}
}

TEST_PLATFORM = {
    "android-4.2-x86": {"build": {"platform": "android"}},
    "android-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-4": {"build": {"platform": "android"}},
    "linux32": {"build": {"platform": "linux32"}},
    "linux64": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "maxosx64"}},
    "windows10-32": {"build": {"platform": "win32", "type": ["ming32"]}},
    "windows10-64": {"build": {"platform": "win64"}},
    "windows7-32": {"build": {"platform": "win32"}},
}

TEST_OPTIONS = {
    o: {"build": {"type": [o]}}
    for o in BUILD_TYPES +[
        "ming32",
        "qr",
        "gradle",
        "mingw32",
        "stylo-disabled",
        "stylo-sequential"
    ]
}
TEST_OPTIONS["nightly"] = {"build": {"train": "nightly"}}
TEST_OPTIONS["devedition"] = {"build": {"train": "devedition"}}

RUN_OPTIONS = {
    "profiling": {"run": {"type": ["profile"]}},
    "e10s": {"run": {"type": ["e10s"]}}
}

TALOS_TEST = {t.replace('_', '-'): {"run": {"suite": t}} for t in KNOWN_PERFHERDER_TESTS}

TEST_SUITE = {
    t: {"run": {"suite": {"name": t}}}
    for t in [
        "awsy",
        "awsy-stylo-disabled",
        "awsy-stylo-sequential",
        "browser-instrumentation",
        "browser-screenshots",
        "cppunit",
        "crashtest",
        "firefox-ui-functional-local",
        "firefox-ui-functional-remote",
        "geckoview",
        "gtest",
        "jittest",
        "jsreftest",
        "marionette",
        "marionette-headless",
        "mochitest",
        "mochitest-a11y",
        "mochitest-browser-chrome",
        "mochitest-browser-screenshots",
        "mochitest-chrome",
        "mochitest-clipboard",
        "mochitest-devtools-chrome",
        "mochitest-jetpack",
        "mochitest-gpu",
        "mochitest-media",
        "mochitest-plain-headless",
        "mochitest-valgrind",
        "mochitest-webgl",
        "mozmill",
        "reftest",
        "reftest-gpu",
        "reftest-no-accel",
        "reftest-stylo",
        "robocop",
        "telemetry-tests-client",
        "test-verify",
        "test-verify-wpt",
        "web-platform-tests",
        "web-platform-tests-reftests",
        "web-platform-tests-wdspec",
        "xpcshell"
    ]
}

TEST_CHUNK = {text_type(i): {"run": {"chunk": i}} for i in range(200)}


BUILD_PLATFORM = {
    p: {"build":{"platform":p}}
    for p in [
        "android-api-16",
        "android-x86",
        "android",
        "linux",
        "macosx64",
        "win32",
        "win64"

    ]
}

BUILD_OPTIONS = {
    "aarch64": {},
    "add-on-devel": {},
    "asan-fuzzing": {"build": {"type": ["asan"]}},
    "asan-reporter": {"build": {"type": ["asan"]}},
    "asan": {"build": {"type": ["asan"]}},
    "base-toolchains": {},
    "ccov": {"build": {"type": ["ccov"]}},
    "checkstyle": {},
    "dmd": {},
    "findbugs": {},
    "fuzzing": {"build": {"type": ["fuzzing"]}},
    "geckoview-docs": {},
    "gradle": {},
    "jsdcov": {"build": {"type": ["jsdcov"]}},
    "lint": {},
    "mingw32": {},
    "noopt": {},
    "old-id": {},
    "rusttests": {"build": {"type": ["rusttests"]}},
    "stylo-only": {"build": {"type": ["stylo-only"]}},
    "test": {},
    "universal": {},
    "without-google-play-services": {}

}

COMPILED_CATEGORIES = {c:[(Matcher(k), v) for k, v in p.items()] for c, p in CATEGORIES.items()}

