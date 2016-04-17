import config
import time
from gcloud import pubsub
from pprint import pprint
from gcloud import datastore
import manuf
import json

debug = True
batch_size = 50
global_counter = 0

def get_client():
    return datastore.Client(config.PROJECT_ID)

def update(data, identifier=None):
    ds = get_client()
    if identifier:
        key = ds.key(config.KIND, int(identifier))
        print "Updating key: %s" %key
    else:
        key = ds.key(config.KIND)
        print "Updating key: %s" %key
    entity = datastore.Entity(key=key)
    entity.update(data)
    ds.put(entity)
    

def mac_lookup(mac):
    p = manuf.MacParser()
    return unicode(p.get_manuf(mac))

def delete(identifier):
    ds = get_client()
    key = ds.key(config.KIND, int(identifier))
    ds.delete(key)


def check_if_exists(mac):
    ds = get_client()
#    entity = datastore.Entity()
    query = ds.query(kind=config.KIND)
    query.add_filter('source','=',mac)
    query.keys_only()
    results = query.fetch()
    if debug:
        print ("Found %d number of instances of mac %s ") %(len(list(results)), mac)
    print(type(len(list(results))))
    print (len(list(results)))
    if len(list(results)) != 0:
        
        return results[0].key.id
    else:
        return None

    #for result in results:
        #return result.key.id
        #return identifier


# Pulls down one message from the queue, checks if the source mac already exists if it does it updates the according ID
#
def get_queue():        
    client = pubsub.Client(project=config.PROJECT_ID)
    topic = client.topic(config.TOPIC)
    subscription = topic.subscription(config.SUBSCRIPTION)
    if not subscription.exists():
        subscription.create()
    received = subscription.pull(max_messages=batch_size)
    #received = subscription.pull()
    ack_ids = []
#Loops through the received messages from the queue and checks if they already exists 
    if debug:
        print "Received %d messages from subscription" %len(received)
        time.sleep(2)
    for recv in received:
        conv = json.loads(recv[1].data)
        mac_fom_queue = conv['source']
        exists = check_if_exists(mac_fom_queue)
        ack_ids.append([recv[0]])
        if  exists == None:
            if debug:
                print("Did not find mac: %s") %mac_fom_queue
        else:
            if debug:
                print("Deleting id: %s because we found mac %s in the database") %(exists, mac_fom_queue)
            delete(exists)
        
        conv['producer'] = mac_lookup(mac_fom_queue)
        update(conv)
        if debug:
            print("Removing %d of messages") %len(ack_ids)
        subscription.acknowledge(ack_ids)
        ack_ids = []

    #if len(ack_ids) > 0:
    #    
    ##    if debug:
     #       print "Removed %d messages from the queue" %len(ack_ids)
     #       time.sleep(2)

def main():
    while True:
        get_queue()
    #delete(4584377761660928)

if __name__=="__main__":
    main()
