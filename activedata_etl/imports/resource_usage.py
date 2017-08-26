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

from future.utils import text_type
from mo_dots import Data, join_field, wrap
from mo_logs import Log
from pyLibrary.env import http


def normalize_resource_usage(url):
    """
    :param url: POOINT TO RESOURCE USAGE FILE
    :return: NORMALIZED RESOURCE USAGE
    """

    return None

    response = http.get(url)
    content = response.content
    if not content.strip():
        return None
    usage = json2value(convert.utf82unicode(content))

    output = Data()
    output.meta.version = usage.version
    output.timing.start = usage.start
    output.timing.end = usage.end
    output.timing.duration = usage.duration

    # ESTABLISH A MAPPING FROM usage TO measure NAMES IN THE FORM
    # (parent_name, child_name, [accessor0, accessor1, ...])
    fields = {k[:-7]: v for k, v in usage.items() if k.endswith("_fields")}
    fields["cpu_times_sum"], fields["cpu_times"] = fields["cpu_times"], []
    measures = [(f, c, [f, i]) for f, g in fields.items() for i, c in enumerate(g)]
    for core, _ in enumerate(usage.overall.cpu_percent_cores):
        measures.append(("cpu_percent_cores", text_type(i), ["cpu_percent_cores", core]))
        for column_number, f in enumerate(fields["cpu_times_sum"]):
            measures.append(("cpu_times", text_type(column_number), ["cpu_times", core, column_number]))
    measures.append(("cpu_percent_mean", ".", []))
    # ASSIGN EACH MEASURE A COLUMN NUMBER, AND PACK THE WHOLE THING INTO A tuple
    measures = tuple((column_number, f, c, k) for column_number, (f, c, k) in enumerate(measures))

    def normalize(samples):
        output = tuple([] for _ in measures)
        if len(samples) > 1000:
            Log.warning("Found {{num}} resource-usage samples", num=len(samples))
        for s in samples:
            timing = {"start": s.start, "end": s.end, "duration": s.duration}
            for column_number, _, _, access_sequence in measures:
                value = s
                for a in access_sequence:
                    value = value[a]
                output[column_number].append({"timing": timing, "value": value})
        return output

    # RUNNING OUT OF MEMORY DURING SERIALIZAITON OF THIS RESULT
    # n = normalize(usage.samples)
    # output.samples = []
    # for column_number, f, c, _ in measures:
    #     output.samples.append({
    #         "measure": join_field([f, c]),
    #         "samples": n[column_number]
    #     })

    usage.phases.append(usage.overall)
    usage.overall.name = "overall"
    output.summary = []
    for p in usage.phases:
        n = normalize(wrap([p]))
        for column_number, f, c, _ in measures:
            one_summary = wrap(n[column_number][0])
            if one_summary.value != None:
                one_summary.phase = p.name
                one_summary.measure = join_field([f, c])
                output.summary.append(one_summary)

    return output


