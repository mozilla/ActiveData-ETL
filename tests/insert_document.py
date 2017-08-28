# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import requests

from pyLibrary import convert
from mo_logs import startup, constants
from mo_logs import Log


def main():
    try:
        settings = startup.read_settings()
        constants.set(settings.constants)
        Log.start(settings.debug)

        response = requests.post(
            "http://localhost:9200/tasks/task/_bulk",
            data=b'{"index":{"_id": "491:66300.10"}}\n' + convert.unicode2utf8(value2json(data)) + b"\n"
        )
        Log.note(response.content)
    except Exception as e:
        Log.error("Problem with insert", e)
    finally:
        Log.stop()


data = {
    "workerType": "tcvcs-cache-device",
    "taskGroupId": "ZUJsaFFpSGaJIf2d-De-5Q",
    "provisionerId": "aws-provisioner-v1",
    "_id": "491:66300.10",
    "extra": {},
    "artifacts": [
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/codeaurora_kernel_msm/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/codeaurora_kernel_msm/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/device-flame/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/device-flame/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/fake-libdvm/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/fake-libdvm/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/fake-qemu-kernel/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/fake-qemu-kernel/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/gonk-misc/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/gonk-misc/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/hardware_qcom_display/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/hardware_qcom_display/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/kernel_lk/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/kernel_lk/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/librecovery/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/librecovery/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/moztt/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/moztt/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_bionic/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_bionic/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_bootable_recovery/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_bootable_recovery/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_build/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_build/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_external_bluetooth_bluedroid/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_external_bluetooth_bluedroid/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_external_libnfc-pn547/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_external_libnfc-pn547/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_frameworks_av/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_frameworks_av/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_hardware_libhardware_moz/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_hardware_libhardware_moz/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_prebuilts_misc/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_prebuilts_misc/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_system_bluetoothd/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_system_bluetoothd/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_system_libfdio/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_system_libfdio/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_system_libpdu/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_system_libpdu/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_system_nfcd/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_system_nfcd/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/platform_system_sensorsd/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/platform_system_sensorsd/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/rilproxy/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/rilproxy/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/valgrind/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/valgrind/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/b2g/vex/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/b2g/vex/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/apitrace/apitrace/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/apitrace/apitrace/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/device/common/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/device/common/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/device/generic/armv7-a-neon/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/device/generic/armv7-a-neon/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/device/qcom/common/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/device/qcom/common/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/device/sample/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/device/sample/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/abi/cpp/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/abi/cpp/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/aac/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/aac/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/bison/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/bison/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/bsdiff/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/bsdiff/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/bzip2/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/bzip2/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/checkpolicy/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/checkpolicy/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/curl/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/curl/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/dhcpcd/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/dhcpcd/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/dnsmasq/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/dnsmasq/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/dropbear/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/dropbear/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/e2fsprogs/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/e2fsprogs/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/elfutils/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/elfutils/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/expat/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/expat/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/fdlibm/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/fdlibm/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/flac/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/flac/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/freetype/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/freetype/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/gcc-demangle/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/gcc-demangle/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/genext2fs/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/genext2fs/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/giflib/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/giflib/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/gtest/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/gtest/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/harfbuzz/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/harfbuzz/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/harfbuzz_ng/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/harfbuzz_ng/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/icu4c/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/icu4c/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/iproute2/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/iproute2/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/ipsec-tools/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/ipsec-tools/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/iptables/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/iptables/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/jack/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/jack/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/jhead/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/jhead/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/jpeg/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/jpeg/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/junit/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/junit/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libgsm/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libgsm/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/liblzf/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/liblzf/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libnfc-nxp/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libnfc-nxp/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libnl-headers/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libnl-headers/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libogg/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libogg/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libpcap/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libpcap/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libpng/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libpng/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libselinux/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libselinux/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libsepol/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libsepol/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/libvpx/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/libvpx/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/mdnsresponder/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/mdnsresponder/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/mksh/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/mksh/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/netcat/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/netcat/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/openssl/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/openssl/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/protobuf/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/protobuf/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/safe-iop/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/safe-iop/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/scrypt/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/scrypt/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/sepolicy/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/sepolicy/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/sfntly/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/sfntly/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/skia/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/skia/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/sonivox/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/sonivox/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/speex/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/speex/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/sqlite/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/sqlite/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/stlport/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/stlport/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/strace/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/strace/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/svox/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/svox/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tagsoup/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tagsoup/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tcpdump/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tcpdump/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tinyalsa/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tinyalsa/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tinycompress/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tinycompress/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tinyxml/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tinyxml/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tinyxml2/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tinyxml2/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/tremolo/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/tremolo/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/webp/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/webp/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/webrtc/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/webrtc/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/wpa_supplicant_8/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/wpa_supplicant_8/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/yaffs2/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/yaffs2/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/external/zlib/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/external/zlib/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/frameworks/base/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/frameworks/base/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/frameworks/native/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/frameworks/native/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/frameworks/opt/emoji/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/frameworks/opt/emoji/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/frameworks/wilhelm/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/frameworks/wilhelm/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/libhardware/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/libhardware/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/libhardware_legacy/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/libhardware_legacy/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/qcom/audio/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/qcom/audio/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/qcom/camera/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/qcom/camera/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/qcom/gps/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/qcom/gps/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/qcom/media/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/qcom/media/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/qcom/wlan/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/qcom/wlan/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/hardware/ril/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/hardware/ril/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/libcore/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/libcore/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/libnativehelper/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/libnativehelper/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/ndk/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/ndk/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/arm/arm-eabi-4.7/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/arm/arm-eabi-4.7/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/arm/arm-linux-androideabi-4.7/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/arm/arm-linux-androideabi-4.7/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/host/i686-linux-glibc2.7-4.6/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/host/i686-linux-glibc2.7-4.6/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/host/x86_64-linux-glibc2.7-4.6/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/host/x86_64-linux-glibc2.7-4.6/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/x86/i686-linux-android-4.7/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/gcc/linux-x86/x86/i686-linux-android-4.7/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/ndk/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/ndk/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/python/linux-x86/2.7.5/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/python/linux-x86/2.7.5/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/sdk/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/sdk/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/prebuilts/tools/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/prebuilts/tools/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/bluetooth/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/bluetooth/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/core/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/core/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/extras/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/extras/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/media/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/media/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/netd/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/netd/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/qcom/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/qcom/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/security/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/security/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/system/vold/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/system/vold/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/vendor/qcom/msm8610/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/vendor/qcom/msm8610/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/caf/platform/vendor/qcom-opensource/wlan/prima/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/caf/platform/vendor/qcom-opensource/wlan/prima/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/external/t2m-foxfone/platform_external_libnfc-nci/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/external/t2m-foxfone/platform_external_libnfc-nci/master.tar.gz"
        },
        {
            "storageType": "s3",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/git.mozilla.org/releases/gaia/master.tar.gz",
            "expires": 1459771785.645,
            "contentType": "application/x-tar",
            "name": "public/git.mozilla.org/releases/gaia/master.tar.gz"
        },
        {
            "storageType": "reference",
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/logs/live.log",
            "expires": 1488716861.292,
            "contentType": "text/plain",
            "name": "public/logs/live.log"
        },
        {
            "contentType": "text/plain",
            "name": "public/logs/live_backing.log",
            "storageType": "s3",
            "expires": 1488716860.391,
            "url": "https://queue.taskcluster.net/v1/task/WPr6KlwJQou9eKHfPjg9eg/artifacts/public/logs/live_backing.log",
            "main_log": True
        }
    ],
    "expires": 1488729923.896,
    "pulse": {
        "status": {
            "workerType": "tcvcs-cache-device",
            "taskGroupId": "ZUJsaFFpSGaJIf2d-De-5Q",
            "runs": [{
                "scheduled": 1457179529,
                "reasonCreated": "scheduled",
                "takenUntil": 1457181872.604,
                "started": 1457179749.089,
                "workerId": "i-99780a5e",
                "state": "completed",
                "workerGroup": "us-west-2a",
                "reasonResolved": "completed",
                "runId": 0,
                "resolved": 1457180862.641
            }],
            "expires": 1488729923.896,
            "retriesLeft": 5,
            "state": "completed",
            "schedulerId": "task-graph-scheduler",
            "deadline": 1457193923.896,
            "taskId": "WPr6KlwJQou9eKHfPjg9eg",
            "provisionerId": "aws-provisioner-v1"
        },
        "_meta": {"count": 66310},
        "workerId": "i-99780a5e",
        "workerGroup": "us-west-2a",
        "version": 1,
        "runId": 0
    },
    "created": 1457179523.896,
    "priority": "normal",
    "schedulerId": "task-graph-scheduler",
    "deadline": 1457193923.896,
    "etl": {
        "source": {
            "name": "Pulse block",
            "timestamp": 1457181506.306,
            "bucket": "active-data-task-cluster-logger-beta",
            "source": {"count": 66300, "name": "pulse.mozilla.org", "id": 66300},
            "type": "aggregation",
            "id": 491
        },
        "type": "join",
        "id": 10,
        "timestamp": 1457181525.053
    },
    "routes": ["index.tc-vcs.v1.repo-project.30511e3d58d82a2f840f2b3f1ab0c456"],
    "retries": 5,
    "scopes": [
        "queue:create-artifact:*",
        "index:insert-task:tc-vcs.v1.repo-project.*"
    ],
    "payload": {
        "features": {"taskclusterProxy": True},
        "maxRunTime": 3600,
        "image": "taskcluster/taskcluster-vcs:2.3.29",
        "cache": [],
        "artifacts": [],
        "command": "\"create-repo-cache\" \"--force-clone\" \"--upload\" \"--proxy\" \"https://git.mozilla.org/b2g/B2G\" \"http://hg.mozilla.org/mozilla-central/raw-file/default/b2g/config/flame-kk/sources.xml\"",
        "env": {"DEBUG": "*"}
    },
    "tags": {},
    "metadata": {
        "owner": "selena@mozilla.com",
        "source": "https://github.com/taskcluster/taskcluster-vcs",
        "name": "cache flame-kk",
        "description": "create-repo-cache --force-clone --upload --proxy https://git.mozilla.org/b2g/B2G http://hg.mozilla.org/mozilla-central/raw-file/default/b2g/config/flame-kk/sources.xml"
    }
}

if __name__ == "__main__":
    main()
