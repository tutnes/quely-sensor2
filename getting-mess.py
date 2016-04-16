from gcloud import pubsub
client = pubsub.Client(        project='practical-brace-126614')
topic = client.topic('topic_name')
subscription = topic.subscription('subscription_name')
with topic.batch() as batch:
     batch.publish(u'this is the first message_payload')
     batch.publish(u'this is the second message_payload',
                  attr1='value1', attr2='value2')
received = subscription.pull()  # API request
messages = [recv[1] for recv in received]
[message.message_id for message in messages]
#[<message_id1>, <message_id2>]
[message.data for message in messages]
#['this is the first message_payload', 'this is the second message_payload']
[message.attributes for message in messages]
#[{}, {'attr1': 'value1', 'attr2': 'value2'}]