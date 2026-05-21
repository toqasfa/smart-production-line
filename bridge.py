#!/usr/bin/env python3
import time
import json
from datetime import datetime
import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, firestore

class MQTTFirestoreBridge:
    def __init__(self):
        # mqtt connection config
        self.MQTT_BROKER = "broker.hivemq.com"
        self.MQTT_PORT = 1883
        self.TOPICS = ["production/weight", "production/rfid", "production/ultrasonic"]
        
        # firebase db config
        self.FIRESTORE_COLLECTION = "production_data"
        self.FIRESTORE_DOCUMENT = "current_status"
        
        # start services
        self.setup_firebase()
        self.setup_mqtt()
        
        # script variables
        self.last_weight = 0
        self.last_rfid = None
        self.detection_flag = False 

    def setup_firebase(self):
        cred = credentials.Certificate("/home/pi/serviceAccountKey.json")
        firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        
        doc_ref = self.db.collection(self.FIRESTORE_COLLECTION).document(self.FIRESTORE_DOCUMENT)
        if not doc_ref.get().exists:
            doc_ref.set({
                "createdAt": datetime.now(),
                "currentWeight": 0,
                "lastRFID": "",
                "lastUpdated": datetime.now(),
                "lastWeight": 0,
                "quantityFaulty": 0,
                "quantityGood": 0,
                "status": "IDLE",
                "totalCount": 0
            })

    def setup_mqtt(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.MQTT_BROKER, self.MQTT_PORT)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, reason_code, properties):
        print("Connected to MQTT broker")
        for topic in self.TOPICS:
            client.subscribe(topic)
            print(f"Subscribed to {topic}")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
            doc_ref = self.db.collection(self.FIRESTORE_COLLECTION).document(self.FIRESTORE_DOCUMENT)
            
            # --- weight topic handler ---
            if msg.topic == "production/weight":
                weight = payload.get("weight", 0)
                updates = {
                    "currentWeight": weight,
                    "lastWeight": weight,
                    "lastUpdated": datetime.now()
                }
                doc_ref.update(updates)
                print(f"Updated weight: {weight}g")

            # --- rfid topic handler ---
            elif msg.topic == "production/rfid":
                rfid_id = payload.get("tag_id", "")
                updates = {
                    "lastRFID": rfid_id,
                    "last_rfid": rfid_id,
                    "quantityGood": firestore.Increment(1),
                    "totalCount": firestore.Increment(1),
                    "lastUpdated": datetime.now(),
                    "status": "NORMAL"
                }
                doc_ref.update(updates)
                print(f"RFID scanned: {rfid_id} (Good count +1)")

            # --- ultrasonic topic handler ---
            elif msg.topic == "production/ultrasonic":
                detected = payload.get("detected", False)
                if detected and not self.detection_flag:
                    updates = {
                        "quantityFaulty": firestore.Increment(1),
                        "totalCount": firestore.Increment(1),
                        "lastUpdated": datetime.now(),
                        "status": "DIVERT"
                    }
                    doc_ref.update(updates)
                    print("Object detected (Faulty count +1)")
                    self.detection_flag = True
                elif not detected:
                    self.detection_flag = False

        except Exception as e:
            print(f"Error processing message: {e}")

    def run(self):
        try:
            print("MQTT to Firestore bridge running...")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("Shutting down...")
            self.client.disconnect()

if __name__ == "__main__":
    bridge = MQTTFirestoreBridge()
    bridge.run()