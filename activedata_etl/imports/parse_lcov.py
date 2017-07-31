# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Tyler Blair (tblair@cs.dal.ca)
#

"""
Parses an lcov-generated coverage file and converts it to the JSON format used by other coverage outputs.
"""

import sys
import json

from mo_dots import wrap
from mo_logs import Log


DEBUG = False
EMIT_RECORDS_WITH_ZERO_COVERAGE = False


def parse_lcov_coverage(stream):
    """
    Parses lcov coverage from a stream
    :param stream:
    :return:
    """
    # XXX BRDA, BRF, BFH not implemented because not used in the output

    current_source = None
    done = set()

    for line in stream:
        line = line.strip()

        if line == 'end_of_record':
            for source in coco_format(current_source):
                if EMIT_RECORDS_WITH_ZERO_COVERAGE:
                    yield source
                elif source.file.total_covered:
                    yield source
            current_source = None
        elif ':' in line:
            cmd, data = line.split(":", 2)

            if cmd == 'TN':
                test_name = data.strip()
                if test_name:
                    Log.warning("Test name found {{name}}", name=test_name)
            elif cmd == 'SF':
                source_file = data
                if source_file in done:
                    Log.error("Note expected to revisit a file")
                current_source = {
                    'file': source_file,
                    'functions': {},
                    'lines_covered': set(),
                    'lines_uncovered': set()
                }
            elif cmd == 'FNF':
                functions_found = int(data)
            elif cmd == 'FNH':
                functions_hit = int(data)
            elif cmd == 'LF':
                lines_found = int(data)
            elif cmd == 'LH':
                lines_hit = int(data)
            elif cmd == 'DA':
                line_number, execution_count = map(int, data.split(","))
                if execution_count > 0:
                    current_source['lines_covered'].add(line_number)
                else:
                    current_source['lines_uncovered'].add(line_number)
            elif cmd == 'FN':
                min_line, function_name = data.split(",", 2)

                current_source['functions'][function_name] = {
                    'start': int(min_line),
                    'execution_count': 0
                }
            elif cmd == 'FNDA':
                fn_execution_count, function_name = data.split(",", 2)
                try:
                    current_source['functions'][function_name]['execution_count'] = int(fn_execution_count)
                except Exception as e:
                    if fn_execution_count != "0":
                        if DEBUG:
                            Log.note("No mention of FN:{{func}}, but it has been called", func=function_name, cause=e)
            else:
                Log.error('Unsupported cmd {{cmd}} with data {{data}}', cmd=cmd, data=data)
        else:
            Log.error("unknown line {{line}}", line=line)


def coco_format(details):
    # TODO: DO NOT IGNORE METHODS
    source = wrap({
        "language": "c/c++",
        "is_file": True,
        "file": {
            "name": details['file'],
            'covered': sorted(details["lines_covered"]),
            'uncovered': sorted(details['lines_uncovered']),
            "total_covered": len(details['lines_covered']),
            "total_uncovered": len(details['lines_uncovered']),
            "percentage_covered": len(details['lines_covered']) / (len(details['lines_covered']) + len(details['lines_uncovered']))
        }
    })

    return [source]


def js_coverage_format(sources):
    results = []
    for key, value in sources.iteritems():
        lines_covered = sorted(value['lines_covered'])

        lines_covered_set = set(lines_covered)

        result = {
            'sourceFile': value['file'],
            'testUrl': value['file'],
            'covered': lines_covered,
            'uncovered': sorted(value['lines_uncovered']),
            'methods': {}
        }

        function_start_lines = sorted([x['start'] for x in value['functions'].values()])

        if len(lines_covered) > 0: # only covered lines are included for methods
            for function_name, function_data in value['functions'].iteritems():
                min_line = function_data['start']
                start_index = function_start_lines.index(min_line)
                max_line = lines_covered[-1] if len(function_start_lines) - 1 == start_index else function_start_lines[start_index + 1]

                function_lines_covered = sorted(lines_covered_set & set(range(min_line, max_line)))

                if len(function_lines_covered) > 0:
                    result['methods'][function_name] = function_lines_covered

        results.append(result)

    return results

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: %s <lcov coverage file>' % sys.argv[0])
        sys.exit(1)

    file_path = sys.argv[1]

    with open(file_path) as f:
        parsed = parse_lcov_coverage(f)

    json.dump(parsed, sys.stdout)
