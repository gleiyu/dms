-- create table
CREATE TABLE test_table (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY,
    name VARCHAR2(100) NOT NULL,
    email VARCHAR2(254),
    PRIMARY KEY (id)
);

INSERT INTO test_table (name, email) VALUES ('张三', 'zhangsan@example.com');
INSERT INTO test_table (name, email) VALUES ('李四', 'lisi@example.com');
INSERT INTO test_table (name, email) VALUES ('王五', 'wangwu@example.com');
INSERT INTO test_table (name, email) VALUES ('赵六', 'zhaoliu@example.com');
INSERT INTO test_table (name, email) VALUES ('孙七', 'sunqi@example.com');
INSERT INTO test_table (name, email) VALUES ('周八', 'zhouba@example.com');
INSERT INTO test_table (name, email) VALUES ('吴九', 'wujiu@example.com');
INSERT INTO test_table (name, email) VALUES ('郑十', 'zhengshi@example.com');
INSERT INTO test_table (name, email) VALUES ('冯十一', 'fengshiyi@example.com');
INSERT INTO test_table (name, email) VALUES ('陈十二', 'chenshier@example.com');

create index idx_test_table_name on test_table(name);

CREATE TABLE sales_orders (
    order_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price NUMBER(10, 2) NOT NULL,
    order_date DATE NOT NULL
);

delete from sales_orders;
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (1, 101, 5, 10.99, TO_DATE('2023-01-05', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (2, 102, 3, 15.99, TO_DATE('2023-01-10', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (3, 101, 7, 10.99, TO_DATE('2023-02-01', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (4, 103, 2, 20.99, TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (5, 103, 2, 20.99, TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (6, 103, 2, 20.99, TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (7, 103, 2, 20.99, TO_DATE('2023-02-15', 'YYYY-MM-DD'));
INSERT INTO sales_orders (order_id, product_id, quantity, price, order_date)
VALUES (8, 103, 2, 20.99, TO_DATE('2023-02-15', 'YYYY-MM-DD'));
-- create view
create view t_test_view as
    select * from test_table;

-- create materialized view

CREATE MATERIALIZED VIEW mv_product_sales
REFRESH COMPLETE ON DEMAND
AS
SELECT product_id, quantity,price
FROM sales_orders;

create materialized view mv_product_sales01
 refresh force on demand start with sysdate next sysdate+1
as select product_id, quantity,price FROM sales_orders
;

create sequence seq_test_01;

-- foreign table
CREATE OR REPLACE DIRECTORY csv_dir AS '/home/oracle/csv';

GRANT READ ON DIRECTORY csv_dir TO pdbadmin;

CREATE TABLE csv_external_table (
  id   NUMBER,
  name VARCHAR2(500),
  age  NUMBER
)
ORGANIZATION EXTERNAL (
  TYPE ORACLE_LOADER
  DEFAULT DIRECTORY csv_dir
  ACCESS PARAMETERS (
    RECORDS DELIMITED BY NEWLINE
    FIELDS TERMINATED BY ','
    MISSING FIELD VALUES ARE NULL
    (
      id   CHAR(10),
      name CHAR(100),
      age  CHAR(10)
    )
  )
  LOCATION ('test.csv')
);