// Runs ONLY on first initialization (empty /data/db)

const DB_NAME = "timeseries_db";
const COLLECTIONS = ["sensor_data", "timeseries_test_db"];

// Switch to the target database
const dbRef = db.getSiblingDB(DB_NAME);

COLLECTIONS.forEach((COLLECTION) => {
  print(`🔧 Creating collection '${COLLECTION}' in DB '${DB_NAME}'`);

  // Create collection explicitly
  dbRef.createCollection(COLLECTION);

  // Indexes for time-series style queries
  dbRef[COLLECTION].createIndex({ time: 1 });

  // Metadata indexes
  dbRef[COLLECTION].createIndex({ "meta.device_id": 1, time: -1 });
  dbRef[COLLECTION].createIndex({ "meta.sensor_id": 1, time: -1 });

  // Geo index (GeoJSON Point)
  dbRef[COLLECTION].createIndex({ location: "2dsphere" });

  print(`✅ Collection '${COLLECTION}' initialized with indexes`);
});


print(`✅ Initialized DB '${DB_NAME}' with collections: ${COLLECTIONS.join(", ")}`);