CREATE TABLE Location(
loc_id    numeric(5) NOT NULL,
state_id  varchar(2) NOT NULL,
store_id  varchar(6) NOT NULL,
CONSTRAINT Location_loc_id_PK PRIMARY KEY (loc_id));

CREATE TABLE Item(
item_dim_id    numeric(6) NOT NULL,
item_id        varchar(64) NOT NULL,
dept_id        varchar(64) NOT NULL,
cat_id         varchar(64) NOT NULL,
sell_price     numeric(5,2) NOT NULL,
CONSTRAINT Item_item_dim_id_PK PRIMARY KEY (item_dim_id));

CREATE TABLE Date(
date_id       numeric(5) NOT NULL,
date          TIMESTAMP(3) NOT NULL,
wm_yr_wk      numeric(5) NOT NULL,
weekday       varchar(9) NOT NULL,
wday          numeric(1) NOT NULL,
month         numeric(2) NOT NULL,
year          numeric(4) NOT NULL,
event_name_1  varchar(32) NOT NULL,
event_name_2  varchar(32) NOT NULL,
CONSTRAINT Date_date_id_PK PRIMARY KEY (date_id));

CREATE TABLE Sales(
date_id         numeric(5) NOT NULL,
loc_id          numeric(5) NOT NULL,
item_dim_id     numeric(6) NOT NULL,
sales_quantity  numeric(4) NOT NULL,
sales_price     numeric(5,2) NOT NULL,
sales_total     numeric(5,2) NOT NULL,
CONSTRAINT Sales_date_id_PK PRIMARY KEY (date_id),
CONSTRAINT Sales_date_id_FK FOREIGN KEY (date_id) REFERENCES Date(date_id),
CONSTRAINT Sales_loc_id_PK PRIMARY KEY (loc_id),
CONSTRAINT Sales_loc_id_FK FOREIGN KEY (loc_id) REFERENCES Location(loc_id),
CONSTRAINT Sales_item_dim_id_PK PRIMARY KEY (item_dim_id),
CONSTRAINT Sales_item_dim_id_FK FOREIGN KEY (item_dim_id) REFERENCES Item(item_dim_id));




ALTER TABLE item
    ALTER COLUMN item_dim_id TYPE numeric(6),
	ALTER COLUMN item_dim_id SET NOT NULL,
    ALTER COLUMN item_id TYPE VARCHAR(64),
	ALTER COLUMN item_id SET NOT NULL,
    ALTER COLUMN dept_id TYPE VARCHAR(64),
	ALTER COLUMN dept_id SET NOT NULL,
    ALTER COLUMN cat_id TYPE VARCHAR(64),
	ALTER COLUMN cat_id SET NOT NULL,
    ALTER COLUMN sell_price TYPE numeric(5,2),
	ALTER COLUMN sell_price SET NOT NULL,
    ADD CONSTRAINT Item_item_dim_id_PK PRIMARY KEY (item_dim_id);

ALTER TABLE location
    ALTER COLUMN loc_id TYPE numeric(5),
	ALTER COLUMN loc_id SET NOT NULL,
    ALTER COLUMN state_id TYPE VARCHAR(2),
	ALTER COLUMN state_id SET NOT NULL,
    ALTER COLUMN store_id TYPE VARCHAR(6),
	ALTER COLUMN store_id SET NOT NULL,
    ADD CONSTRAINT Location_loc_id_PK PRIMARY KEY (loc_id);
