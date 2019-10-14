# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
from __future__ import division, unicode_literals

from collections import Mapping

from activedata_etl.transforms.perfherder_logs_to_perf_logs import (
    KNOWN_PERFHERDER_TESTS,
)
from mo_dots import Data, coalesce, set_default, unwrap
from mo_future import text_type
from mo_hg.hg_mozilla_org import minimize_repo
from mo_logs import Log, strings
from mo_logs.strings import between


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
    task.task.mounts = None
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
                result = p.match(name[len(category) :])
                if result != None:
                    return set_default(result, v)
            else:
                Log.warning(
                    "{{name|quote}} can not be processed with {{category}} for key {{key}}",
                    key=source_key,
                    name=name,
                    category=category,
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
    "source-test-",
)


class Matcher(object):
    def __init__(self, pattern):
        if pattern.startswith("{{"):
            var_name = strings.between(pattern, "{{", "}}")
            self.pattern = globals()[var_name]
            self.literal = None
            remainder = pattern[len(var_name) + 4 :]
        else:
            self.pattern = None
            self.literal = coalesce(strings.between(pattern, None, "{{"), pattern)
            remainder = pattern[len(self.literal) :]

        if remainder:
            self.child = Matcher(remainder)
        else:
            self.child = Data(match=lambda name: None if name else {})

    def match(self, name):
        if self.pattern:
            for k, v in self.pattern.items():
                if isinstance(v, Mapping):
                    # TODO: CONVERT THESE PREFIX MATCHES TO SHORT NAME PULLERS
                    if name.startswith(k):
                        match = self.child.match(name[len(k) :])
                        if match is not None:
                            return set_default(match, v)
                else:
                    l, v = v(name)
                    if v is not None:
                        match = self.child.match(name[l:])
                        if match is not None:
                            return set_default(match, v)

        elif self.literal:
            if name.startswith(self.literal):
                return self.child.match(name[len(self.literal) :])
        return None


CATEGORIES = {
    # TODO: USE A FORMAL PARSER??
    "test-": {
        "debug": {},
        "ui": {},
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "perf"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "perf"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-raptor-{{RAPTOR_TEST}}-{{BROWSER}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-raptor-{{RAPTOR_TEST}}-{{BROWSER}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-{{RAPTOR_TEST}}-{{BROWSER}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-{{RAPTOR_TEST}}-{{BROWSER}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}": {"action": {"type": "test"}},
        # OUTDATED
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-{{BROWSER}}-{{RAPTOR_TEST}}-{{RUN_OPTIONS}}": {
            "action": {"type": "raptor"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-{{BROWSER}}-{{RAPTOR_TEST}}": {
            "action": {"type": "raptor"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "talos"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {
            "action": {"type": "perf"},
            "run": {"framework": "talos"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "talos"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-talos-{{TALOS_TEST}}": {
            "action": {"type": "perf"},
            "run": {"framework": "talos"},
        },
    },
    "build-": {
        "{{BUILD_PLATFORM}}/{{BUILD_TYPE}}": {"action": {"type": "build"}},
        "{{BUILD_PLATFORM}}/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {
            "action": {"type": "build"}
        },
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}/{{BUILD_TYPE}}": {
            "action": {"type": "build"}
        },
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {
            "action": {"type": "build"}
        },
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}-nightly/{{BUILD_TYPE}}": {
            "build": {"train": "nightly"},
            "action": {"type": "build"},
        },
        "{{BUILD_PLATFORM}}-{{BUILD_OPTIONS}}-nightly/{{BUILD_TYPE}}-{{BUILD_STEPS}}": {
            "build": {"train": "nightly"},
            "action": {"type": "build"},
        },
        "{{SPECIAL}}": {"action": {"type": "build"}},
    },
    "desktop-test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "run": {"type": ["chunked"]},
            "action": {"type": "test"},
        },
    },
}

TEST_PLATFORM = {
    "android-4.2-x86": {"build": {"platform": "android"}},
    "android-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-4.3-arm7-api-15": {"build": {"platform": "android"}},
    "android-em-4.2-x86": {"build": {"platform": "android"}},
    "android-em-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-em-7.0-x86": {"build": {"platform": "android"}},
    "android-em-7.0-x86_64": {"build": {"platform": "android"}},
    "android-hw-g5-7-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-android": {"build": {"platform": "android"}},
    "android-4": {"build": {"platform": "android"}},
    "android-7.0-x86": {"build": {"platform": "android"}},
    "android-emu-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-pix-7-1-android-aarch64": {
        "build": {"cpu": "aarch64", "platform": "android"}
    },
    "linux32": {"build": {"platform": "linux32"}},
    "linux64": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "macosx64"}},
    "macosx1010-64": {"build": {"platform": "macosx64"}},
    "macosx1014-64": {"build": {"platform": "macosx64"}},
    "windows8-64": {"build": {"platform": "win64"}},
    "windows10-32": {"build": {"platform": "win32"}},
    "windows10-64-ref-hw-2017": {"build": {"platform": "win64"}},
    "windows10-64": {"build": {"platform": "win64"}},
    "windows10": {"build": {"platform": "win64"}},
    "windows7-32": {"build": {"platform": "win32"}},
}

RUN_OPTIONS = {
    "1proc": {"run": {"type": ["1proc"]}},
    "profiling": {"run": {"type": ["profile"]}},
    "profiling-e10s": {"run": {"type": ["profile", "e10s"]}},
    "profiling-1proc": {"run": {"type": ["profile", "1proc"]}},
    "e10s": {"run": {"type": ["e10s"]}},
    "e10": {"run": {"type": ["e10s"]}},  # TYPO
    "e10s-spi": {"run": {"type": ["e10s", "spi"]}},
    "fis-e10s": {"run": {"type": ["e10s", "fis"]}},
    "fis": {"run": {"type": ["fis"]}},  # fission
    "gpu-1proc": {"run": {"type": ["gpu", "1proc"]}},
    "gpu-e10s": {"run": {"type": ["gpu", "e10s"]}},
    "gpu": {"run": {"type": ["gpu"]}},
    "no-accel-1proc": {"run": {"type": ["no-accel", "1proc"]}},
    "no-accel-e10s": {"run": {"type": ["no-accel", "e10s"]}},
    "no-accel": {"run": {"type": ["no-accel"]}},
    "spi-1proc": {"run": {"type": ["1proc", "spi"]}},
    "spi-e10s": {"run": {"type": ["e10s", "spi"]}},
    "spi": {"run": {"type": ["spi"]}},
    "stylo": {"build": {"type": ["stylo"]}},
    "stylo-1proc": {"build": {"type": ["stylo"]}, "run": {"type": ["1proc"]}},
    "stylo-e10s": {"build": {"type": ["stylo"]}, "run": {"type": ["e10s"]}},
    "stylo-disabled": {"build": {"type": ["stylo-disabled"]}},
    "stylo-disabled-e10s": {
        "build": {"type": ["stylo-disabled"]},
        "run": {"type": ["e10s"]},
    },
    "stylo-sequential-1proc": {"run": {"type": ["1proc"]}},
    "stylo-sequential-e10s": {"run": {"type": ["e10s"]}},
    "stylo-sequential": {},
    "sw-e10s": {"run": {"type": ["service-worker", "e10s"]}},
    "sw-1proc": {"run": {"type": ["service-worker", "1proc"]}},
    "sw": {"run": {"type": ["service-worker"]}},
}

TALOS_TEST = {
    t.replace("_", "-"): {"run": {"suite": t}} for t in KNOWN_PERFHERDER_TESTS
}

RAPTOR_TEST = {
    t: {"run": {"suite": {"name": t}}}
    for t in [
        "ares6",
        "assorted-dom",
        "gdocs",
        "jetstream2",
        "motionmark-animometer",
        "motionmark-htmlsuite",
        "motionmark",
        "scn-cpu-memory-idle",
        "scn-cpu-idle",
        "scn-power-idle-bg",
        "scn-power-idle",
        "stylebench",
        "speedometer",
        "sunspider",
        "unity-webgl",
        "wasm-godot-cranelift",
        "wasm-godot-ion",
        "wasm-godot",
        "wasm-misc-cranelift",
        "wasm-misc-baseline",
        "wasm-misc-ion",
        "wasm-misc",
        "webaudio",
        "youtube-playback",
    ]
}


def match_tp6(name):
    for suite in ["tp6", "tp6m"]:
        prefix = suite + "-"
        if name.startswith(prefix):
            for b in BROWSER.keys():
                if "-" + b in name:
                    short_name = between(name, None, "-" + b)
                    suffix = short_name[len(prefix) :]
                    if suffix in TEST_CHUNK:
                        return (
                            len(short_name),
                            {"run": {"suite": {"name": suite}, "chunk": int(suffix)}},
                        )
                    return len(short_name), {"run": {"suite": {"name": short_name}}}
    return None, None


RAPTOR_TEST["tp6"] = match_tp6
RAPTOR_TEST["tp6m"] = match_tp6

BROWSER = {
    "chrome": {"run": {"browser": "chrome"}},
    "chromium-cold": {"run": {"browser": "chromium"}},
    "chromium": {"run": {"browser": "chromium"}},
    "baseline-firefox": {"run": {"browser": "baseline-firefox"}},
    "fenix-cold": {"run": {"browser": "fenix"}},
    "fenix": {"run": {"browser": "fenix"}},
    "firefox-cold": {"run": {"browser": "firefox"}},
    "firefox": {"run": {"browser": "firefox"}},
    "fennec": {"run": {"browser": "fennec"}},
    "fennec-cold": {"run": {"browser": "fennec"}},
    "fennec64": {"run": {"browser": "fennec"}},
    "fennec64-cold": {"run": {"browser": "fennec"}},
    "geckoview-power": {"run": {"browser": "geckoview"}},
    "geckoview-cpu-memory-power":{"run": {"browser": "geckoview"}},
    "geckoview-cpu-memory": {"run": {"browser": "geckoview"}},
    "geckoview-cpu": {"run": {"browser": "geckoview"}},
    "geckoview-cold": {"run": {"browser": "geckoview"}},
    "geckoview-live": {"run": {"browser": "geckoview"}},
    "geckoview-memory": {"run": {"browser": "geckoview"}},
    "geckoview": {"run": {"browser": "geckoview"}},
    "refbrow-cold": {"run": {"browser": "reference browser"}},
    "refbrow": {"run": {"browser": "reference browser"}},
}


TEST_SUITE = {
    t: {"run": {"suite": {"name": t}}}
    for t in [
        "awsy-base-dmd",
        "awsy-dmd",
        "awsy-base",
        "awsy-tp6",
        "awsy",
        "browser-instrumentation",
        "browser-screenshots",
        "cppunit",
        "crashtest",
        "firefox-ui-functional-local",
        "firefox-ui-functional-remote",
        "geckoview-junit",
        "geckoview-cold",
        "geckoview-memory",
        "geckoview",
        "gtest",
        "jittest",
        "jittgst",  # SPELLING MISTAKE
        "jsreftest",
        "marionette-headless",
        "marionette-media",
        "marionette-stream",
        "marionette",
        "mochitest-a11y",
        "mochitest-browser-chrome",
        "mochitest-browser-screenshots",
        "mochitest-chrome",
        "mochitest-clipboard",
        "mochitest-devtools-webreplay",
        "mochitest-devtools-chrome",
        "mochitest-jetpack",
        "mochitest-gpu",
        "mochitest-media",
        "mochitest-plain-headless",
        "mochitest-remote-sw",
        "mochitest-remote",
        "mochitest-thunderbird",
        "mochitest-valgrind",
        "mochitest-webgl1-core",
        "mochitest-webgl1-ext",
        "mochitest-webgl2-core",
        "mochitest-webgl2-ext",
        "mochitest-webgl",
        "mochitest",
        "mozmill",
        "reftest",
        "reftest-fonts",
        "reftest-gpu",
        "reftest-gpu-fonts",
        "reftest-no-accel",
        "reftest-no-accel-fonts",
        "robocop",
        "telemetry-tests-client",
        "test-coverage",
        "test-coverage-wpt",
        "test-verify",
        "test-verify-wpt",
        "web-platform-tests",
        "web-platform-tests-reftests",
        "web-platform-tests-wdspec",
        "web-platform-tests-wdspec-headless",
        "xpcshell",
    ]
}

TEST_CHUNK = {text_type(i): {"run": {"chunk": i}} for i in range(3000)}

BUILD_PLATFORM = {
    "android-aarch64": {"build": {"platform": "android", "type": ["aarch64"]}},
    "android-geckoview": {"build": {"platform": "android", "product": "geckoview"}},
    "android-hw-g5-7-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-android": {"build": {"platform": "android"}},
    "android-x86": {"build": {"cpu": "x86", "platform": "android"}},
    "android-x86_64": {"build": {"cpu": "x86-64", "platform": "android"}},
    "android-api-16-old-id": {"build": {"platform": "android"}},
    "android-api-16": {"build": {"platform": "android"}},
    "android-api": {"build": {"platform": "android"}},
    "android-test-ccov": {
        "build": {"platform": "android", "type": ["ccov"]},
        "run": {"suite": {"name": "android-test", "fullname": "android-test"}},
    },
    "android": {"build": {"platform": "android"}},
    "fat-aar-android-geckoview": {"build": {"platform": "android", "product": "geckoview"}},
    "linux": {"build": {"platform": "linux"}},
    "linux64": {"build": {"platform": "linux64"}},
    "linux64-dmd": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "macosx64"}},
    "macosx": {"build": {"platform": "maxosx"}},
    "reference-browser": {},
    "win32": {"build": {"platform": "win32"}},
    "win32-dmd": {"build": {"platform": "win32"}},
    "win64": {"build": {"platform": "win64"}},
    "win64-dmd": {"build": {"platform": "win64"}},
}

BUILD_OPTIONS = {
    "aarch64-asan-fuzzing": {"build": {"cpu": "aarch64", "type": ["asan", "fuzzing"]}},
    "aarch64-beta": {"build": {"cpu": "aarch64", "train": "beta"}},
    "aarch64-devedition-nightly": {"build": {"cpu": "aarch64", "train": "devedition"}},
    "aarch64-eme": {
        "build": {"cpu": "aarch64", "type": ["eme"]}
    },  # ENCRYPTED MEDIA EXTENSIONS
    "aarch64-gcp": {"build": {"cpu": "aarch64"}, "run": {"cloud": "gcp"}},
    "aarch64-nightly": {"build": {"cpu": "aarch64", "train": "nightly"}},
    "aarch64-nightly-no-eme": {"build": {"cpu": "aarch64", "train": "nightly"}},
    "aarch64-msvc": {"build": {"cpu": "aarch64"}},
    "aarch64-shippable": {"build": {"cpu": "aarch64", "train": "shippable"}},
    "aarch64-shippable-no-eme": {"build": {"cpu": "aarch64", "train": "shippable"}},
    "aarch64": {"build": {"cpu": "aarch64"}},

    "add-on-devel": {},
    "armel": {"build": {"cpu": "arm"}},
    "armhf": {"build": {"cpu": "arm"}},
    "asan-fuzzing": {"build": {"type": ["asan", "fuzzing"]}},
    "asan-fuzzing-ccov": {"build": {"type": ["asan", "fuzzing", "ccov"]}},
    "asan-reporter": {"build": {"type": ["asan"]}},
    "asan": {"build": {"type": ["asan"]}},
    "base-toolchains": {},
    "base-toolchains-clang": {},
    "beta-test": {"build": {"train": "beta"}},
    "beta": {"build": {"train": "beta"}},
    "ccov": {"build": {"type": ["ccov"]}},
    "fuzzing-ccov": {"build": {"type": ["ccov", "fuzzing"]}},
    "checkstyle": {},
    "debug": {"build": {"type": ["debug"]}},
    "devedition": {"build": {"train": "devedition"}},
    "dmd": {},
    "fat-aar": {},
    "findbugs": {},
    "fuzzing": {"build": {"type": ["fuzzing"]}},
    "gcp": {"run": {"cloud": "gcp"}},
    "gcp-shippable": {"run": {"cloud": "gcp"}, "build": {"train": "shippable"}},
    "geckoNightlyX86Release": {},
    "geckoview-docs": {},
    "gradle": {},
    "jsdcov": {"build": {"type": ["jsdcov"]}},
    "lint": {},
    "lto": {"build": {"type": ["lto"]}},  # LINK TIME OPTIMIZATION
    "mingw32": {},
    "mingwclang": {"build": {"compiler": ["clang"]}},
    "mips": {"build": {"cpu": "mips"}},
    "mipsel": {"build": {"cpu": "mips"}},
    "mips64el": {"build": {"cpu": "mips64"}},
    "msvc": {},
    "no-eme": {},
    "noopt": {},
    "nightly": {"build": {"train": "nightly"}},
    "opt": {"build": {"type": ["opt"]}},
    "old-id": {},
    "ppc64el": {"cpu": "ppc"},
    "pgo": {"build": {"type": ["pgo"]}},
    "plain": {},
    "pytests": {},
    "release-test": {},
    "release": {"build": {"train": "release"}},
    "rusttests": {"build": {"type": ["rusttests"]}},
    "shippable": {"build": {"train": "shippable"}},
    "stylo-only": {"build": {"type": ["stylo-only"]}},
    "s390x": {"build": {"cpu": "s390"}},
    "test": {},
    "tup": {"build": {"type": ["tup"]}},
    "universal": {},
    "without-google-play-services": {},
}

BUILD_TYPE = {
    "opt": {"build": {"type": ["opt"]}},
    "pgo": {"build": {"type": ["pgo"]}},
    "noopt": {"build": {"type": ["noopt"]}},
    "debug": {"build": {"type": ["debug"]}},
    "debug-fennec": {"build": {"type": ["debug"], "product": "fennec"}},
}

TEST_OPTIONS = unwrap(
    set_default(
        {  # NOTICE THESE ALL INCLUDE run.type
            "asan-qr": {"build": {"type": ["asan"]}, "run": {"type": ["qr"]}},
            "gradle": {"run": {"type": ["gradle"]}},
            "lto": {"run": {"type": ["lto"]}},
            "mingw32": {"run": {"type": ["mingw32"]}},
            "ming32": {"run": {"type": ["mingw32"]}},
            "msvc": {"run": {"type": ["msvc"]}},
            "pgo-qr": {"run": {"type": ["qr"]}, "build": {"type": ["pgo"]}},
            "qr": {"run": {"type": ["qr"]}},  # QUANTUM RENDER
            "shippable-qr": {
                "run": {"type": ["qr"]},  # QUANTUM RENDER
                "build": {"train": "shippable"},
            },
            "stylo-disabled": {"run": {"type": ["stylo-disabled"]}},
            "stylo-sequential": {"run": {"type": ["stylo-sequential"]}},
            "ux": {"run": {"type": ["ux"]}},
            "release": {"build": {"train": "release"}},
        },
        BUILD_OPTIONS,
    )
)

BUILD_STEPS = {"upload-symbols": {}}

SPECIAL = {
    "browser-state":{},
    "bundle-debug":{},
    "debug": {},
    "concept-sync": {},
    "feature-session": {},
    "reference-browser-geckoNightlyX86Release": {
        "build": {"product": "reference-browser", "train": "release"}
    },
}

COMPILED_CATEGORIES = {
    c: [(Matcher(k), v) for k, v in p.items()] for c, p in CATEGORIES.items()
}
