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

import re

from pyLibrary import convert, strings
from pyLibrary.debugs.logs import Log
from pyLibrary.dot import wrap, Dict, coalesce, set_default, unwraplist
from pyLibrary.maths import Math
from pyLibrary.times.dates import Date, unicode2datetime


BUILDBOT_LOGS = "http://builddata.pub.build.mozilla.org/builddata/buildjson/"

class BuildbotTranslator(object):

    def __init__(self):
        self.unknown_platforms=[]

    def parse(self, data):
        data = wrap(data)
        output = Dict()

        output.action.start_time = data.starttime
        output.action.end_time = data.endtime
        output.action.request_time = data.requesttime
        output.action.reason = data.reason

        props = data.properties
        if not props:
            return output

        output.run.key = key = props.buildername
        if key.startswith("TB "):
            key = key[3:]

        ratio = RATIO_PATTERN.match(key.split("_")[-1])
        if ratio:
            output.build.step = ratio.groups()[0]

        output.run.timestamp = data.starttime
        output.run.job_number = props.buildnumber
        output.build.revision = props.revision
        output.build.revision12 = props.revision[0:12]
        output.build.product = props.product.lower()
        output.version = props.version

        try:
            output.build.date = Date(unicode2datetime(props.buildid, "%Y%m%d%H%M%S"))
            output.build.id = props.buildid
            props.buildid = None
        except Exception, _:
            output.build.id = "<error>"

        output.build.locale = coalesce(props.locale, 'en-US')
        if props.locales:  # nightly repack build
            output.action.repack = True
            data.build.locale = None
            try:
                data.build.locales = convert.json2value(props.locales).keys()
            except Exception:
                data.build.locales = props.locales.split(",")

        output.build.url = coalesce(props.packageUrl, props.build_url, props.fileURL)
        output.run.logurl = props.log_url
        output.build.release = coalesce(props.en_revision, props.script_repo_revision)
        output.run.machine.name = coalesce(props.slavename, props.aws_instance_id)
        output.run.machine.type = props.aws_instance_type

        try:
            if props.blobber_files:
                files = convert.json2value(props.blobber_files)
                output.run.files = [
                    {"name": name, "url": url}
                    for name, url in files.items()
                ]
        except Exception, e:
            Log.error("Malformed `blobber_files` buildbot property: {{json}}", json=props.blobber_files, cause=e)

        #PRODUCT
        output.build.product = props.product.lower()
        if "xulrunner" in key:
            output.build.product = "xulrunner"

        # PLATFORM
        platform = props.platform
        for vm in VIRTUAL_MACHINES:
            if platform.endswith("_" + vm):
                platform = platform[:-len(vm) - 1]
                output.build.vm = vm
                break
        output.build.platform = platform

        # BRANCH
        output.build.branch = props.branch
        if not output.build.branch:
            Log.error("{{key|quote}} no 'branch' property", key=key)
        branch_name = output.build.branch.split("/")[-1]

        output.run.buildbot_status = STATUS_CODES[data.result]

        if 'release' in key:
            output.tags += ['release']
        if key.endswith("nightly"):
            output.tags += ["nightly"]
        if "Code Coverage " in key:
            if output.build.platform.endswith("-cc"):
                output.build.platform = output.build.platform[:-3]
            else:
                Log.error("Not recognized: {{key}} in \n{{data|json}}", key=key, data=data)
            key = key.replace("Code Coverage ", "")
            output.tags += ["code coverage"]

        for b in ACTIONS:
            expected = strings.expand_template(b, {
                "branch": branch_name,
                "platform": output.build.platform,
                "product": output.build.product,
                "vm": output.build.vm,
                "step": output.build.step,
            })
            if key == expected:
                output.build.name = props.buildername
                scrub_known_properties(props)
                output.other = props
                return output

        if key.startswith("fuzzer"):
            pass
        elif 'l10n' in key or 'repack' in key:
            output.action.repack = True
        elif key.startswith("jetpack-"):
            for t in BUILD_TYPES:
                if key.endswith("-" + t):
                    output.build.type += [t]

            match = re.match(strings.expand_template(
                "jetpack-(.*)-{{platform}}-{{type}}",
                {
                    "platform": output.build.platform,
                    "type": unwraplist(output.build.type)
                }
            ), key)

            if not match:
                Log.error("Not recognized: {{key}} in \n{{data|json}}", key=key, data=data)

            if branch_name == "addon-sdk":
                output.build.branch = match.groups()[0]
        elif key.endswith("nightly"):
            try:
                output.build.name = props.buildername
                platform, build = key.split(" " + branch_name + " ")
                set_default(output, PLATFORMS[platform])

                for t in BUILD_TYPES:
                    if t in build:
                        output.build.type += [t]
            except Exception:
                Log.error("Not recognized: {{key}} in \n{{data|json}}", key=key, data=data)

        elif key.endswith("build"):
            try:
                output.build.name = props.buildername
                platform, build = key.split(" " + branch_name + " ")
                set_default(output, PLATFORMS[platform])
            except Exception, e:
                raise Log.error("Not recognized: {{key}} in \n{{data|json}}", key=key, data=data)

            for t in BUILD_FEATURES:
                if t in build:
                    output.tags += [t]
            for t in BUILD_TYPES:
                if t in build:
                    output.build.type += [t]
        elif key.endswith("valgrind"):
            output.build.name = props.buildername
            platform, build = key.split(" " + branch_name + " ")
            set_default(output, PLATFORMS[platform])
        else:
            # FORMAT: <platform> <branch> <test_mode> <test_name> <other>
            try:
                platform, test = key.split(" " + branch_name + " ")
            except Exception:
                Log.error("Not recognized: {{key}}\n{{data}}", key=key, data=data)

            output.build.name = platform
            if platform not in PLATFORMS:
                if platform not in self.unknown_platforms:
                    self.unknown_platforms += [platform]
                    Log.error("Platform not recognized: {{platform}}\n{{data}}", platform=platform, data=data)
                else:
                    return None  # ERROR INGNORED, ALREADY SENT

            set_default(output, PLATFORMS[platform])

            parsed = parse_test(test, output)
            if not parsed:
                Log.error("Test mode not recognized: {{key}}\n{{data|json}}", key=key, data=data)

        scrub_known_properties(props)
        output.other = props

        if "e10s" in key.lower() and output.run.type != 'e10s':
            Log.error("Did not pickup e10s in\n{{data|json}}", data=data)

        return output


def parse_test(test, output):
    # "web-platform-tests-e10s-7"
    test = test.lower()

    # CHUNK NUMBER
    path = test.split("-")
    if Math.is_integer(path[-1]):
        output.run.chunk = int(path[-1])
        test = "-".join(path[:-1])

    if "-e10s" in test:
        test = test.replace("-e10s", "")
        output.run.type = "e10s"

    for m, d in test_modes.items():
        if test.startswith(m):
            set_default(output, d)
            output.run.suite = test[len(m):].strip()
            return True

    return False

def scrub_known_properties(props):
    props.aws_instance_id = None
    props.aws_instance_type = None
    props.blobber_files = None
    props.branch = None
    props.buildername = None
    # props.buildid = None   #KEEP THE BAD ONE
    props.buildnumber = None
    props.build_url = None
    props.fileURL = None
    props.locale = None
    props.locales = None
    props.log_url = None
    props.packageUrl = None
    props.platform = None
    props.product = None
    props.revision = None
    props.slavename = None
    props.version = None

    for k, v in props.request_times.items():
        props.requests += [{"request_id": int(k), "timestamp": v}]
    props.request_times = None
    props.request_ids = None


test_modes = {
    "debug test": {"build": {"type": "debug"}},
    "opt test": {"build": {"type": "opt"}},
    "pgo test": {"build": {"type": "pgo"}},
    "pgo talos": {"build": {"type": "pgo"}, "run": {"talos": True}},
    "talos": {"run": {"talos": True}}
}

ACTIONS = [
    'b2g_{{branch}}_{{platform}}-debug_periodic',
    'b2g_{{branch}}_{{platform}}_dep',
    'b2g_{{branch}}_{{platform}}_periodic',
    'b2g_{{branch}}_{{platform}} build',
    'b2g_{{branch}}_{{platform}}_nightly',
    'b2g_{{branch}}_{{platform}} nightly',
    'b2g_{{branch}}_emulator-debug_dep',
    'b2g_{{branch}}_emulator_dep',
    # 'b2g_{{branch}}_linux64-b2g-haz_dep',
    'linux64-br-haz_{{branch}}_dep',
    'graphene_{{branch}}_{{platform}} build',
    'graphene_{{branch}}_linux64 build',
    '{{branch}}-{{product}}_{{platform}}_build',
    '{{branch}}-{{product}}_antivirus',
    '{{branch}}-{{product}}_beta_ready_for_beta-cdntest_testing',
    '{{branch}}-{{product}}_beta_ready_for_release',
    '{{branch}}-{{product}}_beta_start_uptake_monitoring',
    '{{branch}}-{{product}}_beta_updates',
    '{{branch}}-{{product}}_bouncer_submitter',
    '{{branch}}-{{product}}_checksums',
    '{{branch}}-{{product}}_push_to_mirrors',
    '{{branch}}-{{product}}_postrelease',
    '{{branch}}-{{product}}_reset_schedulers',
    '{{branch}}-{{product}}_release_ready_for_release-cdntest_testing',
    '{{branch}}-{{product}}_release_ready_for_release',
    '{{branch}}-{{product}}_release_start_uptake_monitoring',
    '{{branch}}-{{product}}_release_updates',
    '{{branch}}-{{product}}_source',
    '{{branch}}-{{product}}_tag_source',
    '{{branch}}-{{platform}}_build',
    '{{branch}}-{{platform}}_update_verify_beta_{{step}}',
    '{{branch}}-{{platform}}_update_verify_release_{{step}}',
    '{{branch}}-{{platform}}_ui_update_verify_beta_{{step}}',
    '{{branch}}-beta_final_verification',
    '{{branch}}-check_permissions',
    '{{branch}} hg bundle',
    '{{branch}}-release_final_verification',
    '{{branch}}-update_shipping_beta',
    '{{branch}}-update_shipping_release',
    '{{branch}}-xr_postrelease',
    '{{platform}}_{{branch}}_dep',
    '{{platform}} {{branch}} periodic file update',
    'Linux x86-64 {{branch}} periodic file update',  # THE platform DOES NOT MATCH
    '{{vm}}_{{branch}}_{{platform}} nightly',
    '{{vm}}_{{branch}}_{{platform}} build'
]

BUILD_TYPES = [
    "opt",
    "pgo",
    "debug",
    "asan"
]

VIRTUAL_MACHINES = [
    "graphene",
    "horizon"
]

BUILD_FEATURES = [
    "leak test",
    "static analysis"
]

PLATFORMS = {
    "Android 4.0 armv7 API 11+": {"run": {"machine": {"os": "android 4.0"}}, "build": {"platform": "arm7"}},
    "Android 4.2 x86": {"run": {"machine": {"os": "android 4.2"}}, "build": {"platform": "x86 emulator"}},
    "Android 4.2 x86 Emulator": {"run": {"machine": {"os": "android 4.2"}}, "build": {"platform": "x86 emulator"}},
    "Android 4.3 armv7 API 11+": {"run": {"machine": {"os": "android 4.3"}}, "build": {"platform": "arm7"}},
    "Android armv7 API 11+": {"run": {"machine": {"os": "android 3.0"}}, "build": {"platform": "arm7"}},
    "Android armv7 API 9": {"run": {"machine": {"os": "android 2.3"}}, "build": {"platform": "arm7"}},
    "b2g_b2g-inbound_emulator_dep": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_ubuntu64_vm": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator64"}},
    "b2g_emulator_vm": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_emulator_vm_large": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_mozilla-central_emulator_nightly": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_mozilla-central_flame-kk_nightly": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "flame-kk"}},
    "b2g_mozilla-inbound_emulator_dep": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_mozilla-inbound_emulator-debug_dep": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator-debug"}},
    "b2g_try_emulator_dep": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator"}},
    "b2g_try_emulator-debug_dep": {"run": {"machine": {"os": "b2g"}}, "build": {"platform": "emulator-debug"}},
    "Linux": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32"}},
    "Linux x86-64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "OS X 10.7": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64"}},
    "OS X 10.7 64-bit": {"run": {"machine": {"os": "lion 10.7"}}, "build": {"platform": "macosx64"}},
    "OS X Mulet": {"run": {"machine": {"os": "mulet"}}, "build": {"platform": "macosx"}},
    "Rev5 MacOSX Yosemite 10.10": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "Rev5 MacOSX Yosemite 10.10.5": {"run": {"machine": {"os": "yosemite 10.10"}}, "build": {"platform": "macosx64"}},
    "Rev4 MacOSX Snow Leopard 10.6": {"run": {"machine": {"os": "snowleopard 10.6"}}, "build": {"platform": "macosx64"}},
    "Rev5 MacOSX Mountain Lion 10.8": {"run": {"machine": {"os": "mountain lion 10.10"}}, "build": {"platform": "macosx64"}},
    "Ubuntu ASAN VM large 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64-vm", "type": "asan"}},
    "Ubuntu ASAN VM 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64-vm", "type": "asan"}},
    "Ubuntu HW 12.04": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32"}},
    "Ubuntu HW 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64"}},
    "Ubuntu VM 12.04": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux32-vm"}},
    "Ubuntu VM 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64-vm"}},
    "Ubuntu VM large 12.04 x64": {"run": {"machine": {"os": "ubuntu"}}, "build": {"platform": "linux64-vm", "type": "asan"}},
    "Windows XP 32-bit": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win32"}},
    "Windows 7 32-bit": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win32"}},
    "Windows 8 64-bit": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "Windows 10 64-bit":{"run": {"machine": {"os": "win10"}}, "build": {"platform": "win64"}},
    "WINNT 5.2": {"run": {"machine": {"os": "winxp"}}, "build": {"platform": "win64"}},
    "WINNT 6.1 x86-64": {"run": {"machine": {"os": "win7"}}, "build": {"platform": "win64"}},
    "WINNT 6.2": {"run": {"machine": {"os": "win8"}}, "build": {"platform": "win64"}},
    "Win32 Mulet": {"run": {"machine": {"os": "mulet"}}, "build": {"platform": "win32"}},
}

STATUS_CODES = {
    0: "success",
    1: "warnings",
    2: "failure",
    3: "skipped",
    4: "exception",
    5: "retry",
    6: "cancelled",
    "0": "success",
    "1": "warnings",
    "2": "failure",
    "3": "skipped",
    "4": "exception",
    "5": "retry",
    "6": "cancelled",
    None: None,
    "success (0)": "success",
    "warnings (1)": "warnings",
    "failure (2)": "failure",
    "skipped (3)": "skipped",
    "exception (4)": "exception",
    "retry (5)": "retry",
    "cancelled (6)": "cancelled"
}

RATIO_PATTERN = re.compile(r"(\d+/\d+)")

