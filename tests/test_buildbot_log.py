from pyLibrary import convert
from pyLibrary.debugs.logs import Log
from pyLibrary.env import http
from pyLibrary.testing.fuzzytestcase import FuzzyTestCase
from testlog_etl.transforms.pulse_block_to_job_logs import parse_builder_message


class TestBuildbotLogs(FuzzyTestCase):
    def test_parse_builder_message(self):
        result = http.get_json(
            "http://activedata.allizom.org/query",
            json={
                "from": "jobs.action.timings",
                "edges": ["builder.step"],
                "limit": 100000,
                "format": "list"
           }
        )

        for d in result.data:
            message, parts = parse_builder_message(d.builder.step, "")
            Log.note("From: {{from_}}\n  To: {{to_}}", from_=d.builder.step, to_=convert.value2json([message, parts]))
