# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import division
from __future__ import unicode_literals

import json
import unittest

import requests

from pyLibrary import jsons
from pyLibrary.testing import elasticsearch

null = None
true = True
false = False

ID = "tc.480019:48001141.30"
TASK_ID = "ER70OtGBQla6YOW5qeivnw"
DATA = {
	"task":{
		"signing":{"cert":[null]},
		"features":["taskclusterProxy","relengAPIProxy","chainOfTrust"],
		"maxRunTime":36000,
		"image":{
			"path":"public/image.tar.zst",
			"type":"task-image",
			"taskId":"ZqrJzMZ8QOOEoltAG7Y4JA"
		},
		"deadline":1484417664.023,
		"id":"ER70OtGBQla6YOW5qeivnw",
		"group":{"id":"aEtr0ac7RA-qeANl9Sa63A"},
		"artifacts":[
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/host/bin/mar",
				"expires":1515867264.023,
				"contentType":"application/octet-stream",
				"name":"public/build/host/bin/mar"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/host/bin/mbsdiff",
				"expires":1515867264.023,
				"contentType":"application/octet-stream",
				"name":"public/build/host/bin/mbsdiff"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/mozharness.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/mozharness.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.checksums",
				"expires":1515867264.023,
				"contentType":"application/octet-stream",
				"name":"public/build/target.checksums"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.common.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.common.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.cppunittest.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.cppunittest.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.gtest.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.gtest.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.json",
				"expires":1515867264.023,
				"contentType":"application/json",
				"name":"public/build/target.json"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.jsshell.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.jsshell.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.langpack.xpi",
				"expires":1515867264.023,
				"contentType":"application/x-xpinstall",
				"name":"public/build/target.langpack.xpi"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.mochitest.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.mochitest.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.mozinfo.json",
				"expires":1515867264.023,
				"contentType":"application/json",
				"name":"public/build/target.mozinfo.json"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.reftest.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.reftest.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.talos.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.talos.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.tar.bz2",
				"expires":1515867264.023,
				"contentType":"application/x-bzip2",
				"name":"public/build/target.tar.bz2"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.test_packages.json",
				"expires":1515867264.023,
				"contentType":"application/json",
				"name":"public/build/target.test_packages.json"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.txt",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/build/target.txt"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.web-platform.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.web-platform.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target.xpcshell.tests.zip",
				"expires":1515867264.023,
				"contentType":"application/zip",
				"name":"public/build/target.xpcshell.tests.zip"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/target_info.txt",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/build/target_info.txt"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/build/xvfb/xvfb.log",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/build/xvfb/xvfb.log"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/chainOfTrust.json.asc",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/chainOfTrust.json.asc"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/logs/certified.log",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/logs/certified.log"
			},
			{
				"storageType":"reference",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/logs/live.log",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/logs/live.log"
			},
			{
				"storageType":"s3",
				"url":"http://queue.taskcluster.net/v1/task/ER70OtGBQla6YOW5qeivnw/artifacts/public/logs/live_backing.log",
				"expires":1515867264.023,
				"contentType":"text/plain",
				"name":"public/logs/live_backing.log"
			}
		],
		"priority":"normal",
		"state":"completed",
		"version":1,
		"env":[
			{"name":"MOZ_BUILD_DATE","value":"20170113181247"},
			{"name":"MH_BUILD_POOL","value":"taskcluster"},
			{
				"name":"HG_STORE_PATH",
				"value":"/home/worker/checkouts/hg-store"
			},
			{
				"name":"GECKO_HEAD_REV",
				"value":"d7e148db2e85f74bef3680e2ad1797c1af5d28f3"
			},
			{"name":"MH_BRANCH","value":"mozilla-inbound"},
			{"name":"MOZ_SCM_LEVEL","value":"3"},
			{
				"name":"MOZHARNESS_ACTIONS",
				"value":"get-secretsbuildcheck-testgenerate-build-statsupdate"
			},
			{
				"name":"GECKO_HEAD_REPOSITORY",
				"value":"https://hg.mozilla.org/integration/mozilla-inbound/"
			},
			{
				"name":"GECKO_BASE_REPOSITORY",
				"value":"https://hg.mozilla.org/mozilla-central"
			},
			{
				"name":"TOOLTOOL_CACHE",
				"value":"/home/worker/tooltool-cache"
			},
			{"name":"NEED_XVFB","value":"true"},
			{
				"name":"MOZHARNESS_CONFIG",
				"value":"builds/releng_base_linux_64_builds.pybalrog/production.py"
			},
			{"name":"USE_SCCACHE","value":"1"},
			{"name":"MH_CUSTOM_BUILD_VARIANT_CFG","value":"asan-tc"},
			{
				"name":"MOZHARNESS_SCRIPT",
				"value":"mozharness/scripts/fx_desktop_build.py"
			}
		],
		"scopes":[
			"secrets:get:project/taskcluster/gecko/hgfingerprint",
			"docker-worker:relengapi-proxy:tooltool.download.public",
			"secrets:get:project/releng/gecko/build/level-3/*",
			"assume:project:taskcluster:level-3-sccache-buckets",
			"docker-worker:cache:level-3-mozilla-inbound-build-linux64-asan-opt-workspace",
			"docker-worker:cache:level-3-checkouts-v1",
			"docker-worker:cache:tooltool-cache"
		],
		"run":{
			"scheduled":1484331266.766,
			"status":"completed",
			"start_time":1484331268.005,
			"worker":{"group":"us-east-1c","id":"i-08916e573568d0074"},
			"state":"completed",
			"reason_created":"scheduled",
			"end_time":1484332765.847,
			"duration":1497.8419997692108
		},
		"tags":[
			{"name":"createdForUser","value":"npierron@mozilla.com"},
			{"name":"owner","value":"npierron@mozilla.com"},
			{
				"name":"source",
				"value":"https://hg.mozilla.org/integration/mozilla-inbound//file/d7e148db2e85f74bef3680e2ad1797c1af5d28f3/taskcluster/ci/build"
			},
			{"name":"description","value":"Linux64OptASAN"},
			{"name":"index.rank","value":"1484331167"},
			{
				"name":"treeherderEnv",
				"value":"[\"production\",\"staging\"]"
			},
			{
				"name":"chainOfTrust.inputs.docker-image",
				"value":"ZqrJzMZ8QOOEoltAG7Y4JA"
			},
			{"name":"onExitStatus","value":"{\"retry\":[4]}"}
		],
		"expires":1515867264.023,
		"worker":{
			"group":"us-east-1c",
			"type":"gecko-3-b-linux",
			"id":"i-08916e573568d0074"
		},
		"dependencies":"ZqrJzMZ8QOOEoltAG7Y4JA",
		"scheduler":{"id":"gecko-level-3"},
		"retries":{"total":5,"remaining":5},
		"runs":[{
			"scheduled":1484331266.766,
			"status":"completed",
			"start_time":1484331268.005,
			"worker":{"group":"us-east-1c","id":"i-08916e573568d0074"},
			"state":"completed",
			"reason_created":"scheduled",
			"end_time":1484332765.847,
			"duration":1497.8419997692108
		}],
		"created":1484331264.023,
		"command":"\"/home/worker/bin/run-task\"\"--chown-recursive\"\"/home/worker/workspace\"\"--chown-recursive\"\"/home/worker/tooltool-cache\"\"--vcs-checkout\"\"/home/worker/workspace/build/src\"\"--tools-checkout\"\"/home/worker/workspace/build/tools\"\"--\"\"/home/worker/workspace/build/src/taskcluster/scripts/builder/build-linux.sh\"",
		"provisioner":{"id":"aws-provisioner-v1"},
		"routes":[
			"index.gecko.v2.mozilla-inbound.latest.firefox.linux64-asan-opt",
			"index.gecko.v2.mozilla-inbound.pushdate.2017.01.13.20170113181247.firefox.linux64-asan-opt",
			"index.gecko.v2.mozilla-inbound.revision.d7e148db2e85f74bef3680e2ad1797c1af5d28f3.firefox.linux64-asan-opt",
			"tc-treeherder.v2.mozilla-inbound.d7e148db2e85f74bef3680e2ad1797c1af5d28f3.85683",
			"tc-treeherder-stage.v2.mozilla-inbound.d7e148db2e85f74bef3680e2ad1797c1af5d28f3.85683"
		],
		"requires":"all-completed"
	},
	"run":{
		"machine":{
			"platform":"linux64",
			"aws_instance_type":"m4.4xlarge",
			"tc_worker_type":"gecko-3-b-linux"
		},
		"timestamp":1484331268.005,
		"name":"build-linux64-asan/opt",
		"suite":{}
	},
	"repo":{
		"changeset":{
			"files":[
				"js/src/jit/BacktrackingAllocator.cpp",
				"js/src/jit/CodeGenerator.cpp",
				"js/src/jit/RegisterSets.h",
				"js/src/jit/Registers.h",
				"js/src/jit/StupidAllocator.cpp",
				"js/src/jit/arm/Architecture-arm.h",
				"js/src/jit/arm64/Architecture-arm64.h",
				"js/src/jit/mips-shared/Architecture-mips-shared.h",
				"js/src/jit/shared/Architecture-shared.h",
				"js/src/jit/x86-shared/Architecture-x86-shared.h",
				"js/src/jsapi-tests/testJitRegisterSet.cpp",
				"js/src/wasm/WasmBaselineCompile.cpp"
			],
			"description":"Bug1321521-RegisterSets:AddaregistertypetogetAnyandaddtheequivalenthasAnyfunction.r=lth",
			"author":"NicolasB.Pierron<nicolas.b.pierron@mozilla.com>",
			"id12":"d7e148db2e85",
			"date":1484331120,
			"id":"d7e148db2e85f74bef3680e2ad1797c1af5d28f3"
		},
		"index":329324,
		"parents":"3ea2d688da6bc524195633d81e2ff00d6bde0fa5",
		"branch":{
			"last_used":1484265976,
			"description":"WhereGeckobugsareintroducedbeforetheyaremergedintomozilla-central",
			"parent_name":"Sourcecodeintegrationwork",
			"url":"https://hg.mozilla.org/integration/mozilla-inbound",
			"locale":"en-US",
			"etl":{"timestamp":1484266909.121088},
			"name":"mozilla-inbound"
		},
		"push":{
			"date":1484331167,
			"user":"npierron@mozilla.com",
			"id":85683
		},
		"etl":{"timestamp":1484332247.83488}
	},
	"build":{
		"revision12":"d7e148db2e85",
		"platform":"linux64",
		"branch":"mozilla-inbound",
		"date":1484331167,
		"type":"asan",
		"revision":"d7e148db2e85f74bef3680e2ad1797c1af5d28f3"
	},
	"action":{
		"start_time":1484331268.67,
		"timings":[
			{
				"duration":1.0945029258728027,
				"step":"taskcluster(presetup)",
				"start_time":1484331268.67,
				"order":0,
				"end_time":1484331269.764503
			},
			{
				"duration":0.0013020038604736328,
				"step":"setup(prechown)",
				"start_time":1484331269.764503,
				"order":1,
				"end_time":1484331269.765805
			},
			{
				"duration":0.00009703636169433594,
				"step":"chown",
				"start_time":1484331269.765805,
				"order":2,
				"end_time":1484331269.765902
			},
			{
				"duration":0.00037384033203125,
				"step":"setup(postchown)",
				"start_time":1484331269.765902,
				"order":3,
				"end_time":1484331269.766276
			},
			{
				"duration":0.00005412101745605469,
				"step":"taskcluster(prevcs)",
				"start_time":1484331269.766276,
				"order":4,
				"end_time":1484331269.76633
			},
			{
				"duration":46.86156606674194,
				"step":"vcs",
				"start_time":1484331269.76633,
				"order":5,
				"end_time":1484331316.627896
			},
			{
				"duration":0.1000058650970459,
				"step":"taskcluster(pretask)",
				"start_time":1484331316.627896,
				"order":6,
				"end_time":1484331316.727902
			},
			{
				"duration":9.855585098266602,
				"step":"task(premozharness)",
				"start_time":1484331316.727902,
				"order":7,
				"end_time":1484331326.583487
			},
			{
				"duration":1370.1879060268402,
				"start_time":1484331326.583487,
				"order":8,
				"step":"mozharness",
				"end_time":1484332696.771393
			},
			{
				"step":"mozharness",
				"harness":{
					"start_time":1484331326.583487,
					"step":"get-secrets",
					"end_time":1484331327.037259,
					"mode":"running",
					"duration":0.4540748596191406,
					"result":"success"
				},
				"order":9
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00024199485778808594,
					"step":"clobber",
					"end_time":1484331327.037804,
					"start_time":1484331327.037562,
					"mode":"skipping"
				},
				"order":10
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00023102760314941406,
					"step":"clone-tools",
					"end_time":1484331327.038035,
					"start_time":1484331327.037804,
					"mode":"skipping"
				},
				"order":11
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00023508071899414062,
					"step":"checkout-sources",
					"end_time":1484331327.03827,
					"start_time":1484331327.038035,
					"mode":"skipping"
				},
				"order":12
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.0002601146697998047,
					"step":"setup-mock",
					"end_time":1484331327.03853,
					"start_time":1484331327.03827,
					"mode":"skipping"
				},
				"order":13
			},
			{
				"step":"mozharness",
				"harness":{
					"start_time":1484331327.03853,
					"step":"build",
					"end_time":1484332390.096471,
					"mode":"running",
					"duration":1063.058082818985,
					"result":"success"
				},
				"order":14
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00013017654418945312,
					"step":"upload-files",
					"end_time":1484332390.096743,
					"start_time":1484332390.096613,
					"mode":"skipping"
				},
				"order":15
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.0001289844512939453,
					"step":"sendchange",
					"end_time":1484332390.096872,
					"start_time":1484332390.096743,
					"mode":"skipping"
				},
				"order":16
			},
			{
				"step":"mozharness",
				"harness":{
					"start_time":1484332390.096872,
					"step":"check-test",
					"end_time":1484332668.766167,
					"mode":"running",
					"duration":278.6694369316101,
					"result":"success"
				},
				"order":17
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00012803077697753906,
					"step":"valgrind-test",
					"end_time":1484332668.766437,
					"start_time":1484332668.766309,
					"mode":"skipping"
				},
				"order":18
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.0001690387725830078,
					"step":"package-source",
					"end_time":1484332668.766606,
					"start_time":1484332668.766437,
					"mode":"skipping"
				},
				"order":19
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.00013685226440429688,
					"step":"generate-source-signing-manifest",
					"end_time":1484332668.766743,
					"start_time":1484332668.766606,
					"mode":"skipping"
				},
				"order":20
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0.0001289844512939453,
					"step":"multi-l10n",
					"end_time":1484332668.766872,
					"start_time":1484332668.766743,
					"mode":"skipping"
				},
				"order":21
			},
			{
				"step":"mozharness",
				"harness":{
					"start_time":1484332668.766872,
					"step":"generate-build-stats",
					"end_time":1484332696.770415,
					"mode":"running",
					"duration":28.003719091415405,
					"result":"success"
				},
				"order":22
			},
			{
				"step":"mozharness",
				"harness":{
					"start_time":1484332696.770591,
					"step":"update",
					"end_time":1484332696.770995,
					"mode":"running",
					"duration":0.0008020401000976562,
					"result":"success"
				},
				"order":23
			},
			{
				"step":"mozharness",
				"harness":{
					"duration":0,
					"step":"FxDesktopBuild",
					"start_time":1484332696.771393,
					"end_time":1484332696.771393
				},
				"order":24
			},
			{
				"duration":0.15726900100708008,
				"step":"task(postmozharness)",
				"start_time":1484332696.771393,
				"order":25,
				"end_time":1484332696.928662
			},
			{
				"duration":66.48233795166016,
				"step":"taskcluster(posttask)",
				"start_time":1484332696.928662,
				"order":26,
				"end_time":1484332763.411
			}
		],
		"harness_time_zone":0,
		"end_time":1484332763.411,
		"duration":1494.7409999370575,
		"harness_time_skew":0.000102996826171875,
		"etl":{"total_bytes":10616855}
	},
	"_id":"tc.480019:48001141.30",
	"treeherder":{
		"jobKind":"build",
		"groupSymbol":"tc",
		"collection":{"asan":true},
		"machine":{"platform":"linux64"},
		"groupName":"ExecutedbyTaskCluster",
		"tier":1,
		"symbol":"Bo"
	},
	"etl":{
		"machine":{
			"python":"CPython",
			"pid":10644,
			"os":"Windows10",
			"name":"ekyle29792"
		},
		"source":{
			"name":"Pulseblock",
			"timestamp":1484332794.980099,
			"bucket":"active-data-task-cluster-logger",
			"source":{
				"count":48001141,
				"source":{"code":"tc"},
				"type":"join",
				"id":48001141
			},
			"type":"aggregation",
			"id":480019
		},
		"type":"join",
		"id":30,
		"timestamp":1485654025.349
	}
}


class TestES(unittest.TestCase):

    def test_tc_record(self):

        es_settings = jsons.ref.expand({
            "host": "http://localhost",
            "port": 9200,
            "index": "test_es",
            "type": "task",
            "timeout": 300,
            "consistency": "one",
            "schema": {
                "$ref": "//../resources/schema/task_cluster.json"
            },
            "debug": True,
            "limit_replicas": True
        }, "file://./test_es.py")

        es = elasticsearch.Cluster(es_settings).create_index(es_settings)
        es.add({"id": ID, "value": DATA})
        es.refresh()

        # CONFIRM IT EXISTS
        query = {"query": {"filtered": {"filter": {"term": {"_id": ID}}}}}
        while True:
            try:
                result = es.search(query)
                self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Should not happen; expecting data to exists before test is run")
                print("Data exists, ready to test")
                break
            except Exception, e:
                print("waiting for data")

        query = {
            "query": {"filtered": {"filter": {"term": {"task.id": TASK_ID}}}},
            "from": 0,
            "size": 10

        }
        result = es.search(query)
        self.assertGreaterEqual(len(result["hits"]["hits"]), 1, "Expecting a record to be returned")
        self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Expecting particular record")

    def test_tc_record_basic(self):
        requests.delete("http://localhost:9200/test_es/")

        requests.post(
            url="http://localhost:9200/test_es",
            data=json.dumps({
                "mappings": {"task": {"properties": {"task": {
                    "type": "object",
                    "dynamic": True,
                    "properties": {
                        "id": {
                            "type": "string",
                            "index": "not_analyzed",
                            "doc_values": True
                        }
                    }
                }}}}
            })
        )

        # ADD RECORD TO ES
        requests.post(
            url="http://localhost:9200/test_es/task/_bulk",
            data=(
                json.dumps({"index": {"_id": ID}}) + "\n" +
                json.dumps(DATA) + "\n"
            )
        )
        requests.post("http://localhost:9200/test_es/_refresh")

        # CONFIRM IT EXISTS
        query = {"query": {"filtered": {"filter": {"term": {"_id": ID}}}}}
        while True:
            try:
                result = json.loads(
                    requests.post(
                        url="http://localhost:9200/test_es/_search",
                        data=json.dumps(query)
                    ).content.decode('utf8'))
                self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Should not happen; expecting data to exists before test is run")
                print("Data exists, ready to test")
                break
            except Exception:
                print("waiting for data")

        query = {
            "query": {"filtered": {"filter": {"term": {"task.id": TASK_ID}}}},
            "from": 0,
            "size": 10

        }
        result = json.loads(
            requests.post(
                url="http://localhost:9200/test_es/task/_search",
                data=json.dumps(query)
            ).content.decode('utf8'))
        self.assertGreaterEqual(len(result["hits"]["hits"]), 1, "Expecting a record to be returned")
        self.assertEqual(result["hits"]["hits"][0]["_id"], ID, "Expecting particular record")
