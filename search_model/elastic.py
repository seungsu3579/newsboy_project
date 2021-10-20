from elasticsearch import Elasticsearch

es = Elasticsearch('http://3.35.236.157:9200')

def make_index(es, index_name):
    
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            es.indices.create(index=index_name)
            return True
        else:
            es.indices.create(index=index_name)
            return True
    except:
        return False
    
    

def delete_index(es, index_name):

    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        return True
    else:
        return False


def insert_doc(es, index_name, id, doc):
    
    es.create(index=index_name, id=id, body=doc)

print(es.indices.exists(index="test"))
make_index(es, "test")
print(es.indices.exists(index="test"))