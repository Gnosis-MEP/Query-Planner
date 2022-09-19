#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from query_planner.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    PUB_EVENT_TYPE_QUERY_SERVICES_QOS_CRITERIA_RANKED
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_msg(event_data):
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}



def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    # for checking published events output
    qos_ranked_cmd = stream_factory.create(PUB_EVENT_TYPE_QUERY_SERVICES_QOS_CRITERIA_RANKED, stype='streamOnly')

    # for testing sending msgs that the service listens to:
    import ipdb; ipdb.set_trace()
    query_created_cmd = stream_factory.create(LISTEN_EVENT_TYPE_QUERY_CREATED, stype='streamOnly')
    query_created_cmd.write_events(
        new_msg(
            {
                'query_id': 'somequery',
                'parsed_query': {
                    'qos_policies': {
                        'energy_consumption': '10',
                        'latency': '9',
                        'accuracy': '5'
                    },
                },
                'service_chain': ['ObjectDetection', 'ColorDetection'],
            }
        )
    )
    import ipdb; ipdb.set_trace()

    # read published events output
    events = qos_ranked_cmd.read_events()
    print(list(events))
    import ipdb; ipdb.set_trace()


if __name__ == '__main__':
    main()
