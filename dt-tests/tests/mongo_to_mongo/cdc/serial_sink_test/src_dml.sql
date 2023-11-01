use test_db_1

db.tb_1.insertOne({ "name": "a", "age": "1" });
db.tb_1.insertOne({ "name": "b", "age": "2" });
db.tb_1.insertOne({ "name": "c", "age": "3" });
db.tb_1.insertOne({ "name": "d", "age": "4" });
db.tb_1.insertOne({ "name": "e", "age": "5" });

db.tb_2.insertOne({ "name": "a", "age": "1" });
db.tb_2.insertOne({ "name": "b", "age": "2" });
db.tb_2.insertOne({ "name": "c", "age": "3" });
db.tb_2.insertOne({ "name": "d", "age": "4" });
db.tb_2.insertOne({ "name": "e", "age": "5" });

db.tb_1.updateOne({ "age" : "4" }, { "$set": { "name" : "d_1" } });
db.tb_1.updateOne({ "age" : "5" }, { "$set": { "name" : "e_1" } });

db.tb_2.updateOne({ "age" : "1" }, { "$set": { "name" : "a_1" } });
db.tb_2.updateOne({ "age" : "2" }, { "$set": { "name" : "b_1" } });

db.tb_1.deleteOne({ "name": "a", "age": "1" });
db.tb_1.deleteOne({ "name": "b", "age": "2" });

db.tb_2.deleteOne({ "name": "d", "age": "4" });
db.tb_2.deleteOne({ "name": "e", "age": "5" });

use test_db_2

-- insert records with custom defined _id and object_id
db.tb_1.insertMany([{ "name": "a", "age": "1", "_id": "1" }, { "name": "b", "age": "1", "_id": "2" }, { "name": "c", "age": "1" }]);

db.tb_1.updateMany({ "age": "1" },  { "$set": { "age" : "1000" } });

db.tb_1.deleteMany({ "age": "1000" });