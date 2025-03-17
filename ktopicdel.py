from kafka.admin import KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092")
topics = admin_client.list_topics()
admin_client.delete_topics(topics)
print("All topics deleted successfully!")
