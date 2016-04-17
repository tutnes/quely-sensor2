import config
from gcloud import pubsub
from pprint import pprint
from gcloud import datastore
import manuf
import json

batch_size = 1
global_counter = 0

def get_client():
    return datastore.Client(config.PROJECT_ID)

def update(data, id=None):
    ds = get_client()
    if id:
        key = ds.key(config.KIND, int(id))
        print "Got ID Passed"
    else:
        key = ds.key(config.KIND)
        print "Got no ID Passed"

    pprint(id)
    entity = datastore.Entity(key=key)
        #exclude_from_indexes=['description'])
    entity.update(data)
    ds.put(entity)
    

def mac_lookup(mac):
    p = manuf.MacParser()
    #return u"Apple"
    return unicode(p.get_manuf(mac))



def check_if_exists(mac):
    ds = get_client()
#    entity = datastore.Entity()
    query = ds.query(kind=config.KIND)
    query.add_filter('source','=',mac)
    query.keys_only()
    results = query.fetch(1)
    for result in results:
        id = result.key.id
    if len(list(results)) == 0:
        return None
    else:
        return id


# Pulls down one message from the queue, checks if the source mac already exists if it does it updates the according ID
#
def delete(id):
    ds = get_client()
    key = ds.key('Book', int(id))
    ds.delete(key)

def get_queue():        
    client = pubsub.Client(project=config.PROJECT_ID)
    topic = client.topic(config.TOPIC)
    subscription = topic.subscription(config.SUBSCRIPTION)
    if not subscription.exists():
        subscription.create()
    received = subscription.pull(max_messages=batch_size)
    ack_ids = []
 
    for recv in received:
        conv = json.loads(recv[1].data)
        exists = check_if_exists(conv['source'])
        print "from main: " 
        pprint(exists)
        ack_ids.append([recv[0]])
        if  exists == None:
            conv['producer'] = mac_lookup(conv['source'])
            update(conv)
        else:
            delete(id)            
            update(conv,exists)
        
    if len(ack_ids) > 0:
        subscription.acknowledge(ack_ids)
        print "Removed one message from the queue"

def main():
    while True:
        get_queue()
    

if __name__=="__main__":
    main()
