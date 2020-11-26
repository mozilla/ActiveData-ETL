# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
from __future__ import division, unicode_literals

from collections import OrderedDict

from activedata_etl.transforms.perfherder_logs_to_perf_logs import (
    KNOWN_PERFHERDER_TESTS,
)
from mo_dots import Data, coalesce, set_default, unwrap, is_data
from mo_future import text
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
    "build-browser-",
    "build-bundle-",
    "build-concept-",
    "build-debug",
    "build-docker_image-",
    "build-docker-image-",
    "build-feature-",
    "build-lib-",
    "build-release-",
    "build-samples-",
    "build-service-",
    "build-signing-",
    "build-snapshot-",
    "build-support-",
    "build-tooling-",
    "build-ui-",
    "checksums-signing-",
    "Cron task for ",
    "docker-image-",
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
                if is_data(v):
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


CATEGORIES = OrderedDict({
    # TODO: USE A FORMAL PARSER??
    "test-vismet-": {},
    "test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{BROWSER}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        # RAPTOR
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-raptor-tp6-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-raptor-tp6-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-tp6-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-raptor-tp6-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "raptor"},
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
        # BROWSERTIME (browser first)
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{BROWSER}}-{{RAPTOR_TEST}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{BROWSER}}-{{RAPTOR_TEST}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{BROWSER}}-{{RAPTOR_TEST}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{BROWSER}}-{{RAPTOR_TEST}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        # BROWSERTIME
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
        # BROWSERTIME SITES
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-{{RAPTOR_TEST}}-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        # BROWSERTIME tp6m
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-tp6m-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-tp6m-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-tp6m-{{BROWSER}}-{{SITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-browsertime-tp6m-{{BROWSER}}-{{SITE}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-tp6m-{{TEST_CHUNK}}-{{BROWSER}}-{{RUN_OPTIONS}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-browsertime-tp6m-{{TEST_CHUNK}}-{{BROWSER}}": {
            "action": {"type": "perf"},
            "run": {"framework": "browsertime"},
        },


        # BASIC TEST FORMAT
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}": {"action": {"type": "test"}},
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
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
        "{{SPECIAL_TESTS}}": {}
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
        "{{SPECIAL_BUILDS}}": {"action": {"type": "build"}},
    },
    "desktop-test-": {
        "{{TEST_PLATFORM}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
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
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}": {
            "action": {"type": "test"}
        },
        "{{TEST_PLATFORM}}-{{TEST_OPTIONS}}/{{BUILD_TYPE}}-{{TEST_SUITE}}-{{RUN_OPTIONS}}-{{TEST_CHUNK}}": {
            "action": {"type": "test"}
        },
    },
})

CATEGORIES["test-vismet-"] = {
    k: set_default({"run": {"suite": {"type": "vismet"}}}, v)
    for k, v in CATEGORIES["test-"].items()
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
    "android-hw-p2-8-0-android-aarch64": {"build": {"cpu": "aarch64", "platform": "android"}},
    "android-4": {"build": {"platform": "android"}},
    "android-7.0-x86": {"build": {"platform": "android"}},
    "android-emu-4.3-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-pix-7-1-android-aarch64": {
        "build": {"cpu": "aarch64", "platform": "android"}
    },
    "linux32": {"build": {"platform": "linux32"}},
    "linux64": {"build": {"platform": "linux64"}},
    "linux1804-32": {"build": {"platform": "linux32"}},
    "linux1804-64": {"build": {"platform": "linux64"}},
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
    "backlog-e10s": {"type": ["e10s"]},
    "condprof-e10s": {"run": {"type": ["condprof", "e10s"]}},
    "profiling": {"run": {"type": ["profile"]}},
    "profiling-fis-e10s": {"run": {"type": ["profile", "fis", "e10s"]}},
    "profiling-e10s": {"run": {"type": ["profile", "e10s"]}},
    "profiling-1proc": {"run": {"type": ["profile", "1proc"]}},
    "e10s": {"run": {"type": ["e10s"]}},
    "e10s-e10s": {"run": {"type": ["e10s"]}},
    "e10": {"run": {"type": ["e10s"]}},  # TYPO
    "e10s-spi": {"run": {"type": ["e10s", "spi"]}},
    "fis-xorig-e10s": {"run": {"type": ["e10s", "xorig", "e10s"]}},
    "fis-e10s": {"run": {"type": ["e10s", "fis"]}},
    "fis": {"run": {"type": ["fis"]}},  # fission
    "gpu-1proc": {"run": {"type": ["gpu", "1proc"]}},
    "gpu-e10s": {"run": {"type": ["gpu", "e10s"]}},
    "gpu-fis-e10s": {"run": {"type": ["gpu", "fis", "e10s"]}},
    "gpu-sw-e10s": {"run": {"type": ["gpu", "service-worker", "e10s"]}},
    "gpu": {"run": {"type": ["gpu"]}},
    # "headless": {"run": {"type": ["headless"]}},
    # "headless-e10s-e10s": {"run": {"type": ["headless", "e10s"]}},
    "no-accel-1proc": {"run": {"type": ["no-accel", "1proc"]}},
    "no-accel-e10s": {"run": {"type": ["no-accel", "e10s"]}},
    "no-accel": {"run": {"type": ["no-accel"]}},
    "oop-fis-e10s": {"run": {"type": ["oop", "fis", "e10s"]}},
    "oop-e10s": {"run": {"type": ["oop", "e10s"]}},
    "qr-e10s": {"run": {"type": ["e10s", "qr"]}},
    "spi-1proc": {"run": {"type": ["1proc", "spi"]}},
    "spi-nw-1proc": {"run": {"type": ["1proc", "spi"]}},
    "spi-nw-e10s": {"run": {"type": ["spi", "e10s"]}},
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
    "swr": {"run": {"type": ["webrender-sw"]}},
    "swr-e10s": {"run": {"type": ["webrender-sw", "e10s"]}},
    "webgpu-e10s": {"run": {"type": ["webgpu", "e10s"]}},
    "webgpu-spi-nw-e10s": {"run": {"type": ["webgpu", "spi", "e10s"]}},
    "webgpu-sw-e10s": {"run": {"type": ["webgpu", "service-worker", "e10s"]}},
    "webgpu-sw": {"run": {"type": ["webgpu", "service-worker"]}},
    "webgpu-fis-e10s": {"run": {"type": ["webgpu", "fis", "e10s"]}},  # fission
    "webgpu-fis": {"run": {"type": ["webgpu", "fis"]}},  # fission
    "wr-1proc": {"run": {"type": ["webrender", "1proc"]}},  # wr = webrender
    "wr-e10s": {"run": {"type": ["webrender", "e10s"]}},  # wr = webrender
    "wr": {"run": {"type": ["webrender"]}},  # wr = webrender
}

TALOS_TEST = {
    t.replace("_", "-"): {"run": {"suite": t}} for t in KNOWN_PERFHERDER_TESTS
}

RAPTOR_TEST = {
    t: {"run": {"suite": {"name": t}}}
    for t in [
        "ares6",
        "assorted-dom",
        "benchmark-speedometer",
        "gdocs",
        "jetstream2",
        "motionmark-animometer",
        "motionmark-htmlsuite",
        "motionmark",
        "scn-cpu-memory-idle-bg",
        "scn-cpu-memory-idle",
        "scn-cpu-memory-power-idle",
        "scn-cpu-idle",
        "scn-power-idle-bg",
        "scn-power-idle",
        "stylebench",
        "speedometer",
        "sunspider",
        "unity-webgl",
        "wasm-godot-cranelift",
        "wasm-godot-ion",
        "wasm-godot-optimizing",
        "wasm-godot",
        "wasm-misc-cranelift",
        "wasm-misc-baseline",
        "wasm-misc-ion",
        "wasm-misc-optimizing",
        "wasm-misc",
        "webaudio",
        "youtube-playback-av1-sfr",
        "youtube-playback-h264-power",
        "youtube-playback-h264-sfr",
        "youtube-playback-h264-std",
        "youtube-playback-h264",
        "youtube-playback-hfr",
        "youtube-playback-vp9-sfr",
        "youtube-playback-widevine-hfr",
        "youtube-playback-widevine-h264-sfr",
        "youtube-playback-widevine-vp9-sfr",
        "youtube-playback",
    ]
}


def match_tp6(name):
    """
    MATCH tp6-<TEST>-<TEST_CHUNK>-<BROWSER>
    :param name:
    :return:
    """

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

SITE = {
    s: {"run": {"site": s}}
    for s in [
        "allrecipes",
        "amazon-search",
        "amazon",
        "apple",
        "bbc",
        "binast-instagram",
        "bing-search-restaurants",
        "bing-search",
        "bing",
        "booking",
        "cnn-ampstories",
        "cnn",
        "docs",
        "ebay-kleinanzeigen-search",
        "ebay-kleinanzeigen",
        "ebay",
        "espn",
        "expedia",
        "facebook-cristiano",
        "facebook-redesign",
        "facebook",
        "fandom",
        "google-mail",
        "google-maps",
        "google-accounts",
        "google-search-restaurants",
        "google-search",
        "google-sheets",
        "google-slides",
        "google",
        "imdb",
        "imgur",
        "instagram",
        "jianshu",
        # "kleinanzeigen",
        "linkedin",
        "medium-article",
        "microsoft-support",
        "microsoft",
        "netflix",
        "nytimes",
        "office",
        "outlook",
        "paypal",
        "people-article",
        "pinterest",
        "reddit",
        "rumble-fox",
        "sheets",
        "slides",
        "stackoverflow-question",
        "stackoverflow",
        "tumblr",
        "twitter",
        "twitch",
        "urbandictionary-define",
        "yandex",
        "yahoo-mail",
        "yahoo-news",
        "youtube-watch",
        "youtube",
        "web-de",
        "wikia-marvel-e10s",
        "wikipedia"
    ]
}

BROWSER = {
    "cold-performance-test-arm64-v8a": {},  # NOT A CLUE WHAT THIS IS
    "cold-nightly-arm64-v8a":{"run": {"browser": "nightly", "cold_start": True}},
    "cold-nightly-armeabi-v7a": {"run": {"browser": "nightly", "cold_start": True}},
    "chrome-m-cold": {"run": {"browser": "chrome", "cold_start": True}},
    "chrome-cold": {"run": {"browser": "chrome", "cold_start": True}},
    "chrome": {"run": {"browser": "chrome"}},
    "chromium-cold": {"run": {"browser": "chromium", "cold_start": True}},
    "chromium": {"run": {"browser": "chromium"}},
    "baseline-firefox": {"run": {"browser": "baseline-firefox"}},
    "fenix-cold": {"run": {"browser": "fenix", "cold_start": True}},
    "fenix": {"run": {"browser": "fenix"}},
    "firefox-cold-condprof": {
        "run": {"browser": "firefox", "cold_start": True, "type": ["condprof"]}
    },  # https://searchfox.org/mozilla-central/source/testing/condprofile/README.rst
    "firefox-cold": {"run": {"browser": "firefox", "cold_start": True}},
    "firefox-condprof": {"run": {"browser": "firefox", "type": ["condprof"]}},
    "firefox": {"run": {"browser": "firefox"}},
    "fennec": {"run": {"browser": "fennec"}},
    "fennec-cold": {"run": {"browser": "fennec", "cold_start": True}},
    "fennec64": {"run": {"browser": "fennec"}},
    "fennec64-cold": {"run": {"browser": "fennec", "cold_start": True}},
    "fennec68": {"run": {"browser": "fennec"}},
    "fennec68-cold": {"run": {"browser": "fennec", "cold_start": True}},
    "geckoview-power": {"run": {"browser": "geckoview"}},
    "geckoview-cpu-memory-power": {"run": {"browser": "geckoview"}},
    "geckoview-cpu-memory": {"run": {"browser": "geckoview"}},
    "geckoview-cpu": {"run": {"browser": "geckoview"}},
    "geckoview-cold": {"run": {"browser": "geckoview", "cold_start": True}},
    "geckoview-live": {"run": {"browser": "geckoview"}},
    "geckoview-memory": {"run": {"browser": "geckoview"}},
    "geckoview": {"run": {"browser": "geckoview"}},
    "live-chrome-m-cold": {"run": {"browser": "chrome", "cold_start": True}},
    "live-firefox-cold": {"run": {"browser": "firefox", "cold_start": True}},
    "live-firefox": {"run": {"browser": "firefox"}},
    "mobile-fenix": {"run": {"browser": "mobile-fenix"}},
    "mobile-geckoview": {"run": {"browser": "mobile-fenix"}},
    "mobile-refbrow": {"run": {"browser": "refbrow"}},
    "refbrow-cold": {"run": {"browser": "reference browser", "cold_start": True}},
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
        "geckoview-junit-e10s-multi",
        "geckoview-junit-e10s-single",
        "geckoview-junit",
        "geckoview-memory",
        "geckoview",
        "gtest",
        "jittest",
        "jittgst",  # SPELLING MISTAKE
        "jsreftest",
        "marionette-framescript",
        "marionette-headless",
        "marionette-media",
        "marionette-stream",
        "marionette",
        "mochitest-a11y",
        "mochitest-browser-chrome",
        "mochitest-browser-screenshots",
        "mochitest-chrome-gpu",
        "mochitest-chrome",
        "mochitest-clipboard",
        "mochitest-devtools-webreplay",
        "mochitest-devtools-chrome",
        "mochitest-jetpack",
        "mochitest-media-gli",
        "mochitest-media",
        "mochitest-plain-headless",
        "mochitest-plain",
        "mochitest-remote",
        "mochitest-thunderbird",
        "mochitest-valgrind",
        "mochitest-webgl1-core-gli",
        "mochitest-webgl1-core",
        "mochitest-webgl1-ext-gli",
        "mochitest-webgl1-ext",
        "mochitest-webgl2-core-gli",
        "mochitest-webgl2-core",
        "mochitest-webgl2-ext-gli",
        "mochitest-webgl2-ext",
        "mochitest-webgl",
        "mochitest",
        "mozmill",
        "reftest-fonts",
        "reftest-gpu",
        "reftest-gpu-fonts",
        "reftest-no-accel",
        "reftest-no-accel-fonts",
        "reftest",
        "robocop",
        "telemetry-tests-client",
        "test-coverage",
        "test-coverage-wpt",
        "test-verify",
        "test-verify-wpt",
        "web-platform-tests-backlog",
        "web-platform-tests-crashtest",
        "web-platform-tests-crashtests",
        "web-platform-tests-print-reftest",
        "web-platform-tests-reftest",
        "web-platform-tests-reftests",
        "web-platform-tests-wdspec",
        "web-platform-tests-wdspec-headless",
        "web-platform-tests",
        "xpcshell",
    ]
}
TEST_SUITE["geckoview-cold"] = {"run": {"suite": {"name": "geckoview"}}, "cold_start": True}

TEST_CHUNK = {text(i): {"run": {"chunk": i}} for i in range(3000)}

BUILD_PLATFORM = {
    "android-aarch64": {"build": {"platform": "android", "cpu": "aarch64"}},
    "android-aarch64-aws": {"build": {"platform": "android", "cpu": "aarch64"}},
    "android-geckoview": {"build": {"platform": "android", "product": "geckoview"}},
    "android-hw-g5-7-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-gs3-7-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-1-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-arm7-api-16": {"build": {"platform": "android"}},
    "android-hw-p2-8-0-android": {"build": {"platform": "android"}},
    "android-x86": {"build": {"cpu": "x86", "platform": "android"}},
    "android-x86_64": {"build": {"cpu": "x86-64", "platform": "android"}},
    "android-x86-aws": {"build": {"cpu": "x86-64", "platform": "android"}},
    "android-api-16-old-id": {"build": {"platform": "android"}},
    "android-api-16": {"build": {"platform": "android"}},
    "android-api": {"build": {"platform": "android"}},
    "android-test": {
        "build": {"platform": "android"},
        "run": {"suite": {"name": "android-test", "fullname": "android-test"}},
    },
    "android": {"build": {"platform": "android"}},
    "fat-aar-android-geckoview": {
        "build": {"platform": "android", "product": "geckoview"}
    },
    "linux": {"build": {"platform": "linux"}},
    "linux1804-32": {"build": {"platform": "linux32"}},
    "linux1804-64": {"build": {"platform": "linux64"}},
    "linux64": {"build": {"platform": "linux64"}},
    "macosx64": {"build": {"platform": "macosx64"}},
    "macosx": {"build": {"platform": "maxosx"}},
    "reference-browser": {},
    "win32": {"build": {"platform": "win32"}},
    "win64": {"build": {"platform": "win64"}},
}

BUILD_OPTIONS = {
    "aarch64-asan-fuzzing": {"build": {"cpu": "aarch64", "type": ["asan", "fuzzing"]}},
    "aarch64-beta": {"build": {"cpu": "aarch64", "train": "beta"}},
    "aarch64-devedition-nightly": {"build": {"cpu": "aarch64", "train": "devedition"}},
    "aarch64-devedition-no-eme": {"build": {"cpu": "aarch64", "train": "devedition", "type": ["no-eme"]}},
    "aarch64-devedition": {"build": {"cpu": "aarch64", "train": "devedition"}},
    "aarch64-eme": {
        "build": {"cpu": "aarch64", "type": ["eme"]}
    },  # ENCRYPTED MEDIA EXTENSIONS
    "aarch64-gcp": {"build": {"cpu": "aarch64"}, "run": {"cloud": "gcp"}},
    "aarch64-nightly": {"build": {"cpu": "aarch64", "train": "nightly"}},
    "aarch64-nightly-no-eme": {"build": {"cpu": "aarch64", "train": "nightly", "type": ["no-eme"]}},
    "aarch64-msvc": {"build": {"cpu": "aarch64"}},
    "aarch64-shippable": {"build": {"cpu": "aarch64", "train": "shippable"}},
    "aarch64-shippable-no-eme": {"build": {"cpu": "aarch64", "train": "shippable", "type": ["no-eme"]}},
    "aarch64": {"build": {"cpu": "aarch64"}},
    "add-on-devel": {},
    "armel": {"build": {"cpu": "arm"}},
    "armhf": {"build": {"cpu": "arm"}},
    "asan-fuzzing": {"build": {"type": ["asan", "fuzzing"]}},
    "asan-fuzzing-ccov": {"build": {"type": ["asan", "fuzzing", "ccov"]}},
    "asan-reporter": {"build": {"type": ["asan"]}},
    "asan-reporter-shippable": {"build": {"type": ["asan"], "train": "shippable"}},
    "asan": {"build": {"type": ["asan"]}},
    "base-toolchains": {},
    "base-toolchains-clang": {},
    "beta-test": {"build": {"train": "beta"}},
    "beta": {"build": {"train": "beta"}},
    "ccov": {"build": {"type": ["ccov"]}},
    "ccov-qr": {"build": {"type": ["ccov"]}, "run": {"type": ["qr"]}},
    "fuzzing-ccov": {"build": {"type": ["ccov", "fuzzing"]}},
    "checkstyle": {},
    "debug": {"build": {"type": ["debug"]}},
    "devedition": {"build": {"train": "devedition"}},
    "devedition-qr": {"build": {"train": "devedition"}, "run": {"type": ["qr"]}},
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
    "no-eme": {"build": {"type": ["no-eme"]}},  # ENCRYPTED MEDIA EXTENSIONS
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
    "reproduced": {},
    "rusttests": {"build": {"type": ["rusttests"]}},
    "shippable-qr": {"build": {"train": "shippable"}, "run": {"type": ["qr"]}},
    "shippable": {"build": {"train": "shippable"}},
    "stylo-only": {"build": {"type": ["stylo-only"]}},
    "s390x": {"build": {"cpu": "s390"}},
    "test": {},
    "tsan-fuzzing": {"build": {"type": ["tsan", "fuzzing"]}},
    "tsan": {"build": {"type": ["tsan"]}},
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
            "devedition": {"build": {"train": "devedition"}},
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

SPECIAL_BUILDS = {
    "android-test-debug": {},
    "android-test-nightly": {},
    "fat-aar-android-geckoview-fat-aar-shippable/opt": {},
    "fennec-nightly": {},
    "firefox-voice": {},
    "linux64/opt-2": {},  # SOME GARBAGE
    "nightly": {"build": {"train": "nightly"}},
    "nightly-browser-awesomebar": {},
    "nightly-browser-errorpages": {},
    "nightly-browser-icons": {},
    "nightly-browser-menu": {},
    "nightly-browser-state": {},
    "nightly-browser-storage-memory": {},
    "nightly-browser-storage-sync": {},
    "nightly-browser-tabstray": {},
    "nightly-concept-engine": {},
    "nightly-concept-toolbar": {},
    "nightly-feature-accounts-push": {},
    "nightly-feature-addons": {},
    "nightly-feature-app-links": {},
    "nightly-feature-contextmenu": {},
    "nightly-feature-findinpage": {},
    "nightly-feature-logins": {},
    "nightly-feature-media": {},
    "nightly-feature-p2p": {},
    "nightly-feature-privatemode": {},
    "nightly-feature-prompts": {},
    "nightly-feature-push": {},
    "nightly-feature-qr": {},
    "nightly-feature-session": {},
    "nightly-feature-sitepermissions": {},
    "nightly-feature-tabs": {},
    "nightly-feature-toolbar": {},
    "nightly-lib-crash": {},
    "nightly-lib-fetch-httpurlconnection": {},
    "nightly-lib-fetch-okhttp": {},
    "nightly-lib-nearby": {},
    "nightly-lib-push-firebase": {},
    "nightly-service-firefox-accounts": {},
    "nightly-service-fretboard": {},
    "nightly-service-location": {},
    "nightly-service-telemetry": {},
    "nightly-simulation": {},
    "nightly-support-base": {},
    "nightly-support-ktx": {},
    "nightly-support-locale": {},
    "nightly-support-migration": {},
    "nightly-support-sync-telemetry": {},
    "nightly-support-test": {},
    "nightly-support-test-appservices": {},
    "nightly-support-webextensions": {},
    "normandy-devtools": {},
    "notarization-part-1-macosx64-shippable/opt": {},
    "notarization-poller-macosx64-shippable/opt": {},
    "raptor": {},
    "src": {},
    "reference-browser-geckoNightlyX86Release": {
        "build": {"product": "reference-browser", "train": "release"}
    },
}

SPECIAL_TESTS = {
    "android-feature-containers": {},
    "android-feature-pwa": {},
    "android-feature-sitepermissions": {},
    "android-feature-top-sites": {},
    "android-feature-share": {},
    "debug": {},
    "ui-browser": {},
    "ui-glean": {},
    "ui": {},
    "nightly": {},
    "unit-browser-engine-gecko-nightly": {}
}

COMPILED_CATEGORIES = {
    c: [(Matcher(k), v) for k, v in p.items()] for c, p in CATEGORIES.items()
}
