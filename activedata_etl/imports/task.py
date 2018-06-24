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

from mo_future import text_type

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

    task._id = None
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


def decode_metatdata_name(source_key, name):
    if name.startswith(NULL_TASKS):
        return {}

    for category, patterns in COMPILED_CATEGORIES.items():
        if name.startswith(category):
            for p, v in patterns:
                result = p.match(name[len(category):])
                if result != None:
                    return set_default(result, v)
            else:
                Log.warning(
                    "{{name|quote}} can not be processed with {{category}} for key {{key}}",
                    key=source_key,
                    name=name,
                    category=category
                )
                break
    return {}


NULL_TASKS = (
    "Buildbot/mozharness S3 uploader",
    "balrog-",
    "beetmover-",
    "build-signing-",
    "build-docker_image-",
    "build-docker-image-",
    "checksums-signing-",
    "Cron task for ",
    "partials-signing-",
    "partials-",
    "repackage-l10n-",
    "nightly-l10n-",
    "source-test-"
)


class Matcher(object):

    def __init__(self, pattern):
        if pattern.startswith("{{"):
            var_name = strings.between(pattern, "{{", "}}")
            self.pattern = globals()[var_name]
            self.literal = None
            remainder = pattern[len(var_name) + 4:]
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
                    if match is not None:
                        return set_default(match, v)
        elif self.literal:
            if name.startswith(self.literal):
                return self.child.match(name[len(self.literal):])
        return None


CATEGORIES = {
    "test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {"action": {"type": "talos"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}": {"action": {"type": "test"}}
    },
    "build-": {
        "{{BUILD_PLATFORM}}/{{BUILD_TYPE}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-nightly/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {"build": {"trigger": "nightly"}, "action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}/{{BUILD_TYPE}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {"build": {"trigger": "nightly"}, "action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}-nightly/{{BUILD_TYPE}}": {"build": {"trigger": "nightly"}, "action": {"type": "build"}},
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}-nightly/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {"build": {"trigger": "nightly"}, "action": {"type": "build"}}
    },
    "desktop-test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {"run": {"type": ["chunked"]}, "action": {"type": "test"}}
    }
}

TEST_PLATFORM = {
    "android-4.2-x86": {"build": {"platform": "android"}},
    "android-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-4.3-arm7-api-15": {"build": {"platform": "android"}},
    "android-em-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-g5-7-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-android": {"build": {"platform": "android"}},
    "android-4": {"build": {"platform": "android"}},
    "android-7.0-x86": {"build": {"platform": "android"}},
    "android-emu-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-pix-7-1-android-aarch64": {"build": {"platform": "android"}},
    "linux32": {"build": {"platform": "linux32"}},
    "linux64": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "maxosx64"}},
    "windows8-64": {"build": {"platform": "win64"}},
    "windows10-32": {"build": {"platform": "win32", "type": ["ming32"]}},
    "windows10-64": {"build": {"platform": "win64"}},
    "windows7-32": {"build": {"platform": "win32"}},
}

TEST_OPTIONS = {
    o: {"build": {"type": [o]}}
    for o in BUILD_TYPES + [
        "aarch64",
        "asan",
        "gradle",
        "mingw32",
        "ming32",
        "msvc",
        "qr",
        "stylo-disabled",
        "stylo-sequential"
    ]
}
TEST_OPTIONS["nightly"] = {"build": {"train": "nightly"}}
TEST_OPTIONS["devedition"] = {"build": {"train": "devedition"}}

RUN_OPTIONS = {
    "profiling": {"run": {"type": ["profile"]}},
    "profiling-e10s": {"run": {"type": ["profile", "e10s"]}},
    "e10s": {"run": {"type": ["e10s"]}},
    "gpu-e10s": {"run": {"type": ["gpu", "e10s"]}},
    "no-accel-e10s": {"run": {"type": ["no-accel", "e10s"]}},
    "stylo": {"build": {"type": ["stylo"]}},
    "stylo-e10s": {"build": {"type": ["stylo"]}, "run": {"type": ["e10s"]}},
    "stylo-disabled": {"build": {"type": ["stylo-disabled"]}},
    "stylo-disabled-e10s": {"build": {"type": ["stylo-disabled"]}, "run": {"type": ["e10s"]}},
    "stylo-sequential": {},
    "stylo-sequential-e10s": {"run": {"type": ["e10s"]}},
}

TALOS_TEST = {t.replace('_', '-'): {"run": {"suite": t}} for t in KNOWN_PERFHERDER_TESTS}

TEST_SUITE = {
    t: {"run": {"suite": {"name": t}}}
    for t in [
        "awsy-base",
        "awsy",
        "browser-instrumentation",
        "browser-screenshots",
        "cppunit",
        "crashtest",
        "raptor-chrome-speedometer",
        "raptor-chrome-tp6",
        "raptor-firefox-speedometer",
        "raptor-firefox-tp6",
        "firefox-ui-functional-local",
        "firefox-ui-functional-remote",
        "geckoview",
        "geckoview-junit",
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
        "reftest-fonts",
        "reftest-gpu",
        "reftest-gpu-fonts",
        "reftest-no-accel",
        "reftest-no-accel-fonts",
        "robocop",
        "talos-bcv",
        "telemetry-tests-client",
        "test-coverage",
        "test-coverage-wpt",
        "test-verify",
        "test-verify-wpt",
        "web-platform-tests",
        "web-platform-tests-reftests",
        "web-platform-tests-wdspec",
        "xpcshell"
    ]
}

TEST_CHUNK = {text_type(i): {"run": {"chunk": i}} for i in range(2000)}

BUILD_PLATFORM = {
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-android": {"build": {"platform": "android"}},
    "android-x86": {"build": {"platform": "android"}},
    "android-api-16-old-id": {"build": {"platform": "android"}},
    "android-api-16": {"build": {"platform": "android"}},
    "android": {"build": {"platform": "android"}},
    "linux": {"build": {"platform": "linux"}},
    "linux64": {"build": {"platform": "linux64"}},
    "linux64-dmd": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "macosx64"}},
    "macosx": {"build": {"platform": "maxosx"}},
    "win32": {"build": {"platform": "win32"}},
    "win32-dmd": {"build": {"platform": "win32"}},
    "win64": {"build": {"platform": "win64"}},
    "win64-dmd": {"build": {"platform": "win64"}}
}

BUILD_OPTIONS = {
    "aarch64": {},
    "add-on-devel": {},
    "asan-fuzzing": {"build": {"type": ["asan", "fuzzing"]}},
    "asan-reporter": {"build": {"type": ["asan"]}},
    "asan": {"build": {"type": ["asan"]}},
    "base-toolchains": {},
    "ccov": {"build": {"type": ["ccov"]}},
    "checkstyle": {},
    "devedition": {"build": {"train": "devedition"}},
    "dmd": {},
    "findbugs": {},
    "fuzzing": {"build": {"type": ["fuzzing"]}},
    "geckoview-docs": {},
    "gradle": {},
    "jsdcov": {"build": {"type": ["jsdcov"]}},
    "lint": {},
    "lto": {"build": {"type": ["lto"]}},  # LINK TIME OPTIMIZATION
    "mingw32": {},
    "msvc": {},
    "noopt": {},
    "nightly": {},
    "old-id": {},
    "pgo": {"build": {"type": ["pgo"]}},
    "plain": {},
    "pytests": {},
    "rusttests": {"build": {"type": ["rusttests"]}},
    "stylo-only": {"build": {"type": ["stylo-only"]}},
    "test": {},
    "tup": {"build": {"type": ["tup"]}},
    "universal": {},
    "without-google-play-services": {}

}

BUILD_TYPE = {
    "opt": {"build": {"type": ["opt"]}},
    "pgo": {"build": {"type": ["pgo"]}},
    "noopt": {"build": {"type": ["noopt"]}},
    "debug": {"build": {"type": ["debug"]}}
}

BUILD_STEPS = {
    "upload-symbols": {}
}

COMPILED_CATEGORIES = {c: [(Matcher(k), v) for k, v in p.items()] for c, p in CATEGORIES.items()}
