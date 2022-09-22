--------------------------------------------------------------------------------
--ETL the data from the normalized schema into the warehouse schema
--------------------------------------------------------------------------------
--Table creation
CREATE TABLE Week(
wm_yr_wk      numeric(5) PRIMARY KEY);

CREATE TABLE Location(
loc_id       numeric(4) PRIMARY KEY,
store_name     varchar(7),						-- former 'store_id'
state_name    varchar(2));						-- former 'state_id'

CREATE TABLE Product(
product_id     numeric(5) PRIMARY KEY,
product_name   varchar(64) NOT NULL,  -- former 'item_id'
cat_name         varchar(64) NOT NULL,
dept_name        varchar(64) NOT NULL);

CREATE TABLE Price(
price_id       numeric(8) NOT NULL PRIMARY KEY,
wm_yr_wk       numeric(5) NOT NULL,
product_id     numeric(5) NOT NULL,
loc_id         numeric(7) NOT NULL,
sales_price    numeric(6,2) NOT NULL,
current_flag   boolean NOT NULL,
effective_timestamp timestamp(3),
expire_timestamp timestamp(3),
CONSTRAINT fk_wm_yr_wk FOREIGN KEY (wm_yr_wk) REFERENCES Week(wm_yr_wk),
CONSTRAINT fk_loc_id FOREIGN KEY (loc_id) REFERENCES Location(loc_id),
CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES Product(product_id));

CREATE TABLE Category(
pd_cat_id        numeric(5) PRIMARY KEY,
cat_name         varchar(64) NOT NULL,
dept_name        varchar(64) NOT NULL);

CREATE TABLE Time(
time_id       numeric(5) PRIMARY KEY,
date          timestamp(3) NOT NULL,
wm_yr_wk      numeric(5) NOT NULL,
weekday       varchar(9) NOT NULL,
wday          numeric(1) NOT NULL,
month         numeric(2) NOT NULL,
year          numeric(4) NOT NULL,
event_name_1    varchar(64),
event_name_2    varchar(64));

CREATE TABLE Sales(
time_id        numeric(5) NOT NULL,
loc_id         numeric(4) NOT NULL,
product_id     numeric(5) NOT NULL,
price_id       numeric(8) NOT NULL,
sales_price    numeric(6,2) NOT NULL,
sales_quantity numeric(4) NOT NULL,
sales_total    numeric(10,2) NOT NULL,
PRIMARY KEY(time_id, loc_id, product_id, price_id),
CONSTRAINT fk_time_id FOREIGN KEY (time_id) REFERENCES Time(time_id),
CONSTRAINT fk_loc_id FOREIGN KEY (loc_id) REFERENCES Location(loc_id),
CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES Product(product_id),
CONSTRAINT fk_price_id FOREIGN KEY (price_id) REFERENCES Price(price_id));

CREATE TABLE Category_Sales(
wm_yr_wk       numeric(5) NOT NULL,
loc_id         numeric(4) NOT NULL,
pd_cat_id      numeric(5) NOT NULL,
sales_quantity_total numeric(5) NOT NULL,
sales_price_average  numeric(6,2) NOT NULL,
sales_total    numeric(10,2) NOT NULL,
PRIMARY KEY(wm_yr_wk, loc_id, pd_cat_id),
CONSTRAINT fk_wm_yr_wk FOREIGN KEY (wm_yr_wk) REFERENCES Week(wm_yr_wk),
CONSTRAINT fk_loc_id FOREIGN KEY (loc_id) REFERENCES Location(loc_id),
CONSTRAINT fk_pd_cat_id FOREIGN KEY (pd_cat_id) REFERENCES Category(pd_cat_id));

--------------------------------------------------------------------------------
--move data into the warehouse schema
CREATE EXTENSION dblink;
INSERT INTO Location
select * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
        'SELECT store_id, store_name, state_name FROM store
        JOIN state ON store.state_id = state.state_id')
        AS t(loc_id numeric, store_name varchar, state_name varchar);

INSERT INTO Week
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT * FROM Week')
       AS t(wm_yr_wk numeric);

INSERT INTO Product
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT product_id, product_name, cat_name, dept_name FROM product
       JOIN category ON product.pd_cat_id = category.pd_cat_id')
       AS t(product_id numeric, product_name varchar, cat_name varchar, dept_name varchar);

INSERT INTO Category
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT pd_cat_id, cat_name, dept_name FROM category')
       AS t(pd_cat_id numeric, cat_name varchar, dept_name varchar);

INSERT INTO Price
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT price_id, wm_yr_wk , product_id, store_id, sales_price FROM price')
       AS t(price_id numeric, wm_yr_wk numeric, product_id numeric, store_id numeric, sales_price numeric);

INSERT INTO Time
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT date_id, date, wm_yr_wk, weekday, wday, month, year, event.event_name, s.event_name FROM date
       LEFT JOIN event ON date.event_id_1 = event.event_id
       LEFT JOIN (SELECT event_id, event_name FROM event) s ON date.event_id_2 = s.event_id')
       AS t(time_id numeric, date timestamp, wm_yr_wk numeric, weekday varchar,
         wday numeric, month numeric, year numeric, event_name_1 varchar, event_name_2 varchar);

INSERT INTO Sales
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT date_id, sales.store_id, sales.product_id, sales.price_id, sales_price, sales_quantity,
       sales_price*sales_quantity AS sales_total FROM sales
       LEFT JOIN price ON sales.price_id = price.price_id')
       AS t(time_id numeric, loc_id numeric, product_id numeric, price_id numeric, sales_price numeric,
         sales_quantity numeric, sales_total numeric);

INSERT INTO Category_Sales
SELECT * FROM
dblink('host=localhost dbname=CS779_Project_new user=postgres password=Doffy7991',
       'SELECT s.wm_yr_wk, s.loc_id, s.pd_cat_id, s.sales_quantity_total,
       ROUND(AVG(s.sales_total/s.sales_quantity_total),2) AS sales_price_average, s.sales_total
       FROM (SELECT d.wm_yr_wk, sales.store_id AS loc_id, p.pd_cat_id, SUM(sales_quantity) AS sales_quantity_total,
             SUM(sales_price * sales_quantity) AS sales_total
             FROM sales
             LEFT JOIN (SELECT date_id, wm_yr_wk FROM date) d ON sales.date_id = d.date_id
             LEFT JOIN price ON sales.price_id = price.price_id
             LEFT JOIN (SELECT product_id, pd_cat_id FROM product) p ON sales.product_id = p.product_id
             GROUP BY d.wm_yr_wk, sales.store_id, p.pd_cat_id) s
       GROUP BY s.wm_yr_wk, s.loc_id, s.pd_cat_id, s.sales_quantity_total, s.sales_total')
       AS t(wm_yr_wk numeric, loc_id numeric, pd_cat_id numeric, sales_quantity_total numeric,
         sales_price_average numeric, sales_total numeric);

--
