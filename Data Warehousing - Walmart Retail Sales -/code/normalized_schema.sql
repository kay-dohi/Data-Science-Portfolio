--------------------------------------------------------------------------------
--Write queries to move data from flat files to normalized data tables
--------------------------------------------------------------------------------

--Table creation
CREATE TABLE Event(
event_id     numeric(3) PRIMARY KEY,
event_name   varchar(64) NOT NULL,
event_type   varchar(64) NOT NULL);

CREATE TABLE Week(
wm_yr_wk      numeric(5) PRIMARY KEY);

CREATE TABLE Date(
date_id       numeric(5) PRIMARY KEY,
date          timestamp(3) NOT NULL,
wm_yr_wk      numeric(5) NOT NULL,
weekday       varchar(9) NOT NULL,
wday          numeric(1) NOT NULL,
month         numeric(2) NOT NULL,
year          numeric(4) NOT NULL,
event_id_1    numeric(3),
event_id_2    numeric(3),
CONSTRAINT fk_wm_yr_wk  FOREIGN KEY (wm_yr_wk) REFERENCES Week(wm_yr_wk),
CONSTRAINT fk_event_id_1 FOREIGN KEY (event_id_1) REFERENCES Event(event_id),
CONSTRAINT fk_event_id_2 FOREIGN KEY (event_id_2) REFERENCES Event(event_id));

CREATE TABLE State(
state_id      numeric(2) PRIMARY KEY,
state_name    varchar(2));						-- former 'state_id'

CREATE TABLE Store(
store_id       numeric(4) PRIMARY KEY,
store_name     varchar(7),						-- former 'store_id'
state_id       numeric(2) NOT NULL,
CONSTRAINT fk_state_id FOREIGN KEY (state_id) REFERENCES State(state_id));

CREATE TABLE Category(
pd_cat_id      numeric(5) PRIMARY KEY,
cat_name         varchar(64) NOT NULL,
dept_name        varchar(64) NOT NULL);

CREATE TABLE Product(
product_id     numeric(5) PRIMARY KEY,
product_name   varchar(64) NOT NULL,  -- former 'item_id'
pd_cat_id      numeric(5) NOT NULL,
CONSTRAINT fk_pd_cat_id FOREIGN KEY (pd_cat_id) REFERENCES Category(pd_cat_id));

CREATE TABLE Price(
price_id       numeric(8) NOT NULL PRIMARY KEY,
wm_yr_wk       numeric(5) NOT NULL,
product_id     numeric(5) NOT NULL,
store_id       numeric(7) NOT NULL,
sales_price    numeric(6,2) NOT NULL,
CONSTRAINT fk_wm_yr_wk FOREIGN KEY (wm_yr_wk) REFERENCES Week(wm_yr_wk),
CONSTRAINT fk_store_id FOREIGN KEY (store_id) REFERENCES Store(store_id),
CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES Product(product_id));

CREATE TABLE Sales(
sales_id       numeric(8) PRIMARY KEY,
date_id        numeric(5),
store_id       numeric(4),
product_id     numeric(5),
price_id       numeric(8) NOT NULL,
sales_quantity numeric(4) NOT NULL,
CONSTRAINT fk_date_id FOREIGN KEY (date_id) REFERENCES Date(date_id),
CONSTRAINT fk_store_id FOREIGN KEY (store_id) REFERENCES Store(store_id),
CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES Product(product_id),
CONSTRAINT fk_price_id FOREIGN KEY (price_id) REFERENCES Price(price_id));

--create sequence
CREATE SEQUENCE event_seq START WITH 1;
CREATE SEQUENCE date_seq START WITH 1;
CREATE SEQUENCE pd_cat_seq START WITH 1;
CREATE SEQUENCE product_seq START WITH 1;
CREATE SEQUENCE price_seq START WITH 1;
CREATE SEQUENCE sales_seq START WITH 1;
CREATE SEQUENCE pricechange_seq START WITH 1;
CREATE SEQUENCE state_seq START WITH 1;
CREATE SEQUENCE store_seq START WITH 1;

--create indices useful when normalizing
CREATE INDEX sells_item_id ON sells(item_id);
CREATE INDEX sells_dept_id ON sells(dept_id);
CREATE INDEX sells_cat_id ON sells(cat_id);
CREATE INDEX sells_state_id ON sells(state_id);
CREATE INDEX price_item_id ON price(item_id);
CREATE INDEX price_wm_yr_wk ON price(wm_yr_wk);
CREATE INDEX price_sell_price ON price(sell_price);
CREATE INDEX calencar_date ON calendar(date);
CREATE INDEX calencar_wm_yr_wk ON calencar(wm_yr_wk);


--------------------------------------------------------------------------------
--populate schemas
INSERT INTO event(event_id, event_name, event_type)
SELECT nextval('event_seq'), event_name_1, event_type_1
FROM (SELECT distinct event_name_1, event_type_1
	 FROM Calendar WHERE event_name_1 IS NOT NULL) s;

INSERT INTO week(wm_yr_wk)
SELECT distinct wm_yr_wk
FROM Calendar ORDER BY wm_yr_wk;

INSERT INTO date
(date_id, date, wm_yr_wk, weekday, wday, month, year, event_id_1, event_id_2)
SELECT nextval('date_seq'),date, wm_yr_wk, weekday, wday, month, year, event_id_1, event_id
FROM
	(SELECT date, wm_yr_wk, weekday, wday, month, year, event_id_1, event_id
	FROM
	(SELECT date, wm_yr_wk, weekday, wday, month, year, event_id AS event_id_1, event_type_2 FROM calendar
	LEFT JOIN event on calendar.event_name_1 = event.event_name) s
	LEFT JOIN event on s.event_type_2 = event.event_name ORDER BY date) m;

INSERT INTO state(state_id, state_name)
SELECT nextval('state_seq'), s.state_id
FROM (SELECT distinct state_id
	   FROM Sells WHERE state_id IS NOT NULL
     ORDER BY state_id) s;

INSERT INTO store(store_id, store_name, state_id)
SELECT nextval('store_seq'), m.store_id, m.state_id
FROM
    (select s.store_id, state.state_id from state
    join (select distinct store_id, state_id
    FROM sells WHERE store_id IS NOT NULL) s on state.state_name = s.state_id) m
ORDER BY m.store_id

INSERT INTO category(pd_cat_id, cat_name, dept_name)
SELECT nextval('pd_cat_seq'), s.cat_id, s.dept_id
FROM (select distinct cat_id, dept_id
      FROM sells WHERE cat_id IS NOT NULL
      ORDER BY cat_id, dept_id) s

INSERT INTO product(product_id, product_name, pd_cat_id)
SELECT nextval('product_seq'), m.item_id, m.pd_cat_id
FROM
    (select s.item_id, category.pd_cat_id from category
    join (select distinct item_id, dept_id
    FROM sells WHERE item_id IS NOT NULL ORDER BY item_id) s on category.dept_name = s.dept_id) m

INSERT INTO Price(price_id, wm_yr_wk, product_id, store_id, sales_price)
SELECT nextval('price_seq'), wm_yr_wk, product_id, store_id, sell_price
FROM
	(select wm_yr_wk, product_id, store.store_id, sell_price from sellprice
	join store on sellprice.store_id = store.store_name
	join product on sellprice.item_id = product.product_name
	ORDER BY product_id, wm_yr_wk, store_id) s;

INSERT INTO Sales(sales_id, date_id, store_id, product_id, price_id, sales_quantity)
SELECT nextval('sales_seq'), m.date_id, store.store_id, product.product_id, price.price_id, m.sales_quantity
FROM product
	JOIN(
	SELECT item_id, store_id, s.wm_yr_wk, date.date_id, sales_quantity FROM sells
	LEFT JOIN (SELECT CAST(SUBSTRING(d, 3) AS numeric), wm_yr_wk, date FROM calendar) s
	ON sells.date_id = s.substring
	LEFT JOIN date ON s.date = date.date) m ON product.product_name = m.item_id
LEFT JOIN store ON m.store_id = store.store_name
LEFT JOIN price ON m.wm_yr_wk = price.wm_yr_wk
	AND store.store_id = price.store_id
	AND product.product_id = price.product_id
WHERE price_id IS NOT NULL
ORDER BY date_id, store_id, product_id;


--
