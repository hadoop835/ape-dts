INSERT INTO tb_1 VALUES (1, 1, 1, 1);
INSERT INTO tb_1 VALUES (2, 2, 2, 2);

INSERT INTO fk_tb_1 VALUES (1, 1, 1, 1);
INSERT INTO fk_tb_1 VALUES (2, 2, 2, 2);

INSERT INTO fk_tb_2 VALUES (1, 1, 1, 1);
INSERT INTO fk_tb_2 VALUES (2, 2, 2, 2);

INSERT INTO fk_tb_3 VALUES (1, 1, 1, 1);
INSERT INTO fk_tb_3 VALUES (2, 2, 2, 2);

UPDATE tb_1 SET f_3 = 5;
UPDATE fk_tb_1 SET f_3 = 5;
UPDATE fk_tb_2 SET f_3 = 5;
UPDATE fk_tb_3 SET f_3 = 5;

DELETE FROM tb_1;
DELETE FROM fk_tb_3;
DELETE FROM fk_tb_2;
DELETE FROM fk_tb_1;