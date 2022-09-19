
class QoSRankerBase(object):
    def __init__(self, qos_criteria, user_to_sys_qos_map):
        self.qos_criteria = qos_criteria
        self.user_to_sys_qos_map = user_to_sys_qos_map

    def get_query_qos_criteria(self, query):
        for k, v in query['parsed_query']['qos_policies'].items():
            yield (self.user_to_sys_qos_map.get(k, k), v)

    def get_query_services_qos_rank(self, query):
        return {
            'query_id': query['query_id'],
            'required_services': query['service_chain'],
            'qos_rank': self.get_qos_rank(query)
        }

    def get_qos_rank(self, query):
        raise NotImplemented()
