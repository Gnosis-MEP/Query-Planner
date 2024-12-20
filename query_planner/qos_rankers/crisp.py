from query_planner.qos_rankers.base import QoSRankerBase

class CrispQoSRanker(QoSRankerBase):
    def get_qos_rank(self, query):
        qos_rank = {}
        for qos_metric, qos_value in self.get_query_qos_criteria(query):
            qos_rank[qos_metric] = int(qos_value)/10
        return qos_rank