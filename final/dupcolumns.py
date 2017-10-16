es = elasticsearch.Elasticsearch('localhost:9200')2
es.index(index='test_index', doc_type='post', body=s)
import elasticsearch                   
from elasticsearch import helpers

def mangle_dupe_cols(columns):
    counts = {}
    for i, col in enumerate(columns):
        cur_count = counts.get(col, 0)
        if cur_count > 0:
            columns[i] = '%s.%d' % (col, cur_count)
        counts[col] = cur_count + 1
    return columns