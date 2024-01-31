const express = require("express");
const { MongoClient } = require("mongodb");

const app = express();
const port = 3000;

// Connection URL for MongoDB
const url = "mongodb://localhost:27017";
// Database Name
const dbName = "ecom";
// Collection Name
const collectionName = "ecom_collection";

const client = new MongoClient(url);

async function connectToMongoDB() {
  try {
    await client.connect();
    console.log("Connected to MongoDB");
  } catch (error) {
    console.error("Could not connect to MongoDB", error);
  }
}

connectToMongoDB();

app.get("/", (req, res) => {
  res.sendFile(__dirname + "/index.html");
});

app.get("/data/gender-distribution", async (req, res) => {
  try {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const genderData = await collection
      .aggregate([
        { $match: { name: { $ne: null } } },
        { $group: { _id: "$gender", count: { $sum: 1 } } },
      ])
      .toArray();
    res.json(genderData);
  } catch (error) {
    res.status(500).send("Error fetching data from MongoDB");
  }
});

app.get("/data/category-purchases", async (req, res) => {
  try {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const categoryData = await collection
      .aggregate([
        { $match: { name: { $ne: null } } },
        { $group: { _id: "$category", count: { $sum: 1 } } },
      ])
      .toArray();
    res.json(categoryData);
  } catch (error) {
    res.status(500).send("Error fetching data from MongoDB");
  }
});

app.get("/data/age-distribution", async (req, res) => {
  try {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const ageData = await collection
      .find({ name: { $ne: null } })
      .project({ age: 1, _id: 0 })
      .toArray();
    res.json(ageData);
  } catch (error) {
    res.status(500).send("Error fetching data from MongoDB");
  }
});

app.get("/data/user-status", async (req, res) => {
  try {
    const db = client.db(dbName);
    const collection = db.collection(collectionName);
    const statusData = await collection
      .aggregate([
        { $match: { name: { $ne: null } } },
        { $group: { _id: "$user_status", count: { $sum: 1 } } },
      ])
      .toArray();
    res.json(statusData);
  } catch (error) {
    res.status(500).send("Error fetching data from MongoDB");
  }
});



app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});
