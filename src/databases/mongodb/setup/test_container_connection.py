from pymongo import MongoClient
from datetime import datetime

# ---- CONFIG (matches your docker-compose) ----
usr = "mongodb_user"
pwd = "password123"
url = "localhost:27067"
db = "timeseries_db"
col = "sensor_data"
MONGO_URI = db_uri = "mongodb://" + usr + ":" + pwd + "@" + \
    url + "/" + db + "?authSource=admin"
COLLECTION_NAME = "sensor_data"

print("URL is ", MONGO_URI)

def main():
    try:
        print("🔌 Connecting to MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)

        # Force connection check
        client.admin.command("ping")
        print("✅ MongoDB connection OK")

        # db = client[DB_NAME]
        # collection = db[COLLECTION_NAME]

        # # Example document (matches your schema)
        # doc = {
        #     "time": datetime.utcnow(),
        #     "meta": {
        #         "device_id": "demo_device_1",
        #         "sensor_id": "battery_voltage_mv",
        #         "source": "Node 1"
        #     },
        #     "location": {
        #         "type": "Point",
        #         "coordinates": [5.263733, 60.090717]
        #     },
        #     "observations": [
        #         {
        #             "parameter": "battery_voltage_mv",
        #             "value": 3624,
        #             "unit": "mV"
        #         }
        #     ]
        # }

        # print("📥 Inserting test document...")
        # result = collection.insert_one(doc)
        # print("✅ Inserted with _id:", result.inserted_id)

        # print("📤 Reading document back...")
        # found = collection.find_one({"_id": result.inserted_id})
        # print("✅ Found document:")
        # print(found)

    except Exception as e:
        print("❌ MongoDB test failed")
        print(e)

if __name__ == "__main__":
    main()
