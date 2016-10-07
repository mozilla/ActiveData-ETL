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


def parse_lcov_coverage(file_path):
    # XXX BRDA, BRF, BFH not implemented because not needed
    # TODO This should support streams instead, for when eventually input is streamed directly
    # in from lcov via stdout

    sources = {}

    current_source = None

    total_lines_covered = 0
    total_lines_uncovered = 0

    with open(file_path) as f:
        for line in f:
            line = line.strip()

            if line == 'end_of_record':
                current_source = None
            else:
                colon_index = line.index(':')
                cmd = line[0:colon_index]
                data = line[colon_index + 1:]

                if cmd == 'TN':
                    test_name = data
                elif cmd == 'SF':
                    source_file = data

                    if source_file not in sources:
                        sources[source_file] = {
                            'file': source_file,
                            'functions': {},
                            'lines_covered': set(),
                            'lines_uncovered': set(),
                            'line_execution_counts': {}
                        }

                    current_source = sources[source_file]
                elif cmd == 'FNF':
                    functions_found = int(data)
                elif cmd == 'FNH':
                    functions_hit = int(data)
                elif cmd == 'LF':
                    lines_found = int(data)
                elif cmd == 'LH':
                    lines_hit = int(data)
                elif cmd == 'DA':
                    split = data.split(',')
                    line_number = int(split[0])
                    execution_count = int(split[1])

                    current_source['line_execution_counts'][line_number] = execution_count

                    if execution_count > 0:
                        current_source['lines_covered'].add(line_number)
                        total_lines_covered += 1
                    else:
                        current_source['lines_uncovered'].add(line_number)
                        total_lines_uncovered += 1
                elif cmd == 'FN':
                    split = data.split(',')
                    min_line = int(split[0])
                    function_name = split[1]

                    current_source['functions'][function_name] = {
                        'start': min_line,
                        'execution_count': 0
                    }
                elif cmd == 'FNDA':
                    split = data.split(',')
                    execution_count = int(split[0])
                    function_name = split[1]

                    if function_name not in current_source['functions']:
                        # print('Unknown function %s for FNDA' % function_name)
                        continue

                    current_source['functions'][function_name]['execution_count'] = execution_count
                else:
                    print('Unsupported cmd %s with data "%s"' % (cmd, data))

    results = []
    results.append({
        'version': 1
    })

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

    # print('TOTALS L:%d/%d' % (total_lines_covered, total_lines_covered + total_lines_uncovered))
    return results

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: %s <lcov coverage file>' % sys.argv[0])
        sys.exit(1)

    file_path = sys.argv[1]

    parsed = parse_lcov_coverage(file_path)
    json.dump(parsed, sys.stdout)