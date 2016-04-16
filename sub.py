from gcloud import pubsub
client = pubsub.Client(        project='practical-brace-126614')
topic = client.topic('topic_name')
subscription = topic.subscription('subscription_name')
subscription.create()  # API request