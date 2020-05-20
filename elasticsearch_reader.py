import elasticsearch
import elasticsearch.helpers


class ElasticReader:
    def __init__(self, es_host='localhost', es_port=9200, es_index=None):
        self.es_index = es_index
        self.elastic = elasticsearch.Elasticsearch(hosts=[{"host": es_host, "port": es_port}])

    def read_all_ids(self, start_date):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "match_all": {}
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": start_date
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            }}

        ids = set()
        for result in elasticsearch.helpers.scan(self.elastic, index=self.es_index, query=query):
            tid = result['_source']['@fields']['tweet_id']
            ids.add(tid)

        return ids
