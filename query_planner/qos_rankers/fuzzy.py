from query_planner.qos_rankers.base import QoSRankerBase


class FuzzyQoSRanker(QoSRankerBase):
    def __init__(self, qos_criteria, user_to_sys_qos_map):
        super().__init__(qos_criteria, user_to_sys_qos_map)

        self.fuzzy_criteria_rank_variable = {
            'high_importance': (0.7, 0.9, 1.0),
            'medium_high_importance': (0.5, 0.7, 0.9),
            'medium_importance': (0.3, 0.5, 0.7),
            'medium_low_importance': (0.1, 0.3, 0.5),
            'low_importance': (0.0, 0.1, 0.3),
        }

    def get_qos_rank(self, query):
        qos_rank = {}
        for qos_metric, qos_value in self.get_query_qos_criteria(query):
            qos_rank[qos_metric] = self.fuzzy_criteria_rank_variable[qos_value.lower()]
        return qos_rank