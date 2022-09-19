import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer

from query_planner.conf import (
    QOS_CRITERIA,
    USER_TO_SYS_QOS_MAP,
    LISTEN_EVENT_TYPE_QUERY_CREATED,
    PUB_EVENT_TYPE_QUERY_SERVICES_QOS_CRITERIA_RANKED,
)

from query_planner.qos_rankers.crisp import CrispQoSRanker


class QueryPlanner(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 qos_ranker_class,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(QueryPlanner, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.ranker = None
        self.qos_ranker_class = qos_ranker_class
        self.available_qos_rankers = {
            'Crisp': CrispQoSRanker,
            'Fuzzy': CrispQoSRanker
        }
        self.setup_ranker()

        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

    def setup_ranker(self):
        self.ranker = self.available_qos_rankers[self.qos_ranker_class](qos_criteria=QOS_CRITERIA, user_to_sys_qos_map=USER_TO_SYS_QOS_MAP)

    def publish_query_services_qos_criteria_ranked(self, event_data):
        event_data['id'] = self.service_based_random_event_id()
        self.publish_event_type_to_stream(event_type=PUB_EVENT_TYPE_QUERY_SERVICES_QOS_CRITERIA_RANKED, new_event_data=event_data)

    def process_query_created(self, event_data):
        query_services_qos_criteria_ranked = self.ranker.get_query_services_qos_rank(event_data)
        self.publish_query_services_qos_criteria_ranked(query_services_qos_criteria_ranked)

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(QueryPlanner, self).process_event_type(event_type, event_data, json_msg):
            return False
        if event_type == LISTEN_EVENT_TYPE_QUERY_CREATED:
            self.process_query_created(event_data)

    def log_state(self):
        super(QueryPlanner, self).log_state()
        self.logger.info(f'Service name: {self.name}')
        self.logger.info(f'Ranker: {self.qos_ranker_class}')
        self.logger.info(f'QoS Criteria: {QOS_CRITERIA}')

    def run(self):
        super(QueryPlanner, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_thread.start()
        self.cmd_thread.join()
