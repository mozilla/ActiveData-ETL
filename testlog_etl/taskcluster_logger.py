import json
from mozillapulse import consumers
from mozillapulse.config import PulseConfiguration
import taskcluster
import sys


class TaskClusterConsumer(consumers.GenericConsumer):

    def __init__(self, **kwargs):
        super(TaskClusterConsumer, self).__init__(
            PulseConfiguration(**kwargs), 'exchange/taskcluster-queue/v1/task-completed', **kwargs)

def msg_received(data, message):
    message.ack()
    taskid = data.get('status', {}).get('taskId')
    print taskid

    # get the artifact list for the taskId
    tc_queue = taskcluster.Queue()
    artifacts = tc_queue.listLatestArtifacts(taskid)
    print json.dumps(artifacts, indent=2)

    # get the url for the "live.log"
    for artifact in artifacts.get('artifacts', {}):
        if artifact.get('name', '').endswith('live.log'):
            url = tc_queue.buildUrl('getLatestArtifact',
                                    taskid,
                                    artifact['name'])
            print url
            break


def main():
    pulse_cfg = PulseConfiguration(user='jgriffin', password='foobar1')
    pulse = TaskClusterConsumer(applabel="test-tc-consumer", connect=False)
    pulse.config = pulse_cfg
    pulse.configure(topic="#", callback=msg_received, durable=False)
    pulse.listen()

if __name__ == "__main__":
    main()
