from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file 
import base64
import os 
import random
import numpy as np                      # pip install numpy    ##to install
import time

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files=glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=files[0];

# Set the project_id with your project ID
project_id="coudcompproj";
topic_name = "Image2Redis";   # change it for your topic name if needed


# create a publisher and get the topic path for the publisher
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient( publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

folder_path = 'Dataset_Occluded_Pedestrian' 
script_dir = os.path.dirname(os.path.abspath(__file__))
full_folder_path = os.path.join(script_dir, folder_path)

for filename in os.listdir(full_folder_path):
    if filename.endswith('.png'): 
        file_path = os.path.join(full_folder_path, filename)
        
        with open(file_path, "rb") as f:
            value = base64.b64encode(f.read())  
        key = filename
        try:    
            future = publisher.publish(topic_path, value, ordering_key=key);
    		#ensure that the publishing has been completed successfully
            future.result()    
            print("The messages has been published successfully")
        except: 
            print("Failed to publish the message")