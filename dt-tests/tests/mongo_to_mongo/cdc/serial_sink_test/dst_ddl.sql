use test_db_1

db.tb_1.drop()
db.tb_2.drop()

db.createCollection("tb_1");
db.createCollection("tb_2");

use test_db_2

db.tb_1.drop()
db.tb_2.drop()

db.createCollection("tb_1");
db.createCollection("tb_2");
