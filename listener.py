import config
from gcloud import pubsub
from pprint import pprint
from gcloud import datastore
import json

batch_size = 100

def get_client():
    return datastore.Client(config.PROJECT_ID)

def update(data, id=None):
    ds = get_client()
    if id:
        key = ds.key(config.KIND, int(id))
    else:
        key = ds.key(config.KIND)

    entity = datastore.Entity(
        key=key,
        exclude_from_indexes=['description'])

    entity.update(data)
    ds.put(entity)

def check_if_exists(mac):
    ds = get_client()
    #key = ds.key(config.KIND)
    query = ds.query(kind=config.KIND)
    query.add_filter('source','=',"KUKK")
    result = query.fetch()
    list(result)
    return 


client = pubsub.Client(project=config.PROJECT_ID)
topic = client.topic(config.TOPIC)

subscription = topic.subscription(config.SUBSCRIPTION)

if not subscription.exists():
	subscription.create()
received = subscription.pull(max_messages=batch_size)


print len(received)
#pprint(received)

ack_ids = []



for recv in received:
	#print(recv[1].data)
    jk = json.loads(recv[1].data)
    check_if_exists(jk['source'])
    update(jk)
    #update(recv[1].data)
    ack_ids.append([recv[0]])
    
if len(ack_ids) > 0:
    subscription.acknowledge(ack_ids)

# Should update the row if the mac address exists before
# Should add a row if it does not exist before



def mac_lookup(mac):
	return "Apple"



