CREATE TABLE IF NOT EXISTS client (
	client_id INT PRIMARY KEY, 
	bonus_balance FLOAT, 
	category_id INT 
);

CREATE TABLE IF NOT EXISTS category (
	category_id INT PRIMARY KEY, 
	name VARCHAR(200), 
	percent FLOAT, 
	min_payment FLOAT
);

CREATE TABLE IF NOT EXISTS payment (
	payment_id INT PRIMARY KEY, 
	client_id INT, 
	dish_id INT, 
	dish_amount FLOAT,
	order_id INT,
	order_time TIMESTAMP,
	order_sum FLOAT,
	tips FLOAT
);

CREATE TABLE IF NOT EXISTS dish (
	dish_id INT PRIMARY KEY, 
	name VARCHAR(200), 
	price FLOAT
);

CREATE TABLE IF NOT EXISTS logs (
	id INT PRIMARY KEY, 
	table_name VARCHAR(200), 
	time TIMESTAMP,
	values json
);


CREATE SEQUENCE log_id_seq
START WITH 1
INCREMENT BY 1;


--############################3

CREATE OR REPLACE FUNCTION insert_category_log()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO logs (id, time, table_name, values)
    VALUES (nextval('log_id_seq'), now(), 'category', row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER insert_category_trigger
AFTER INSERT ON category
FOR EACH ROW
EXECUTE FUNCTION insert_category_log();

--#################################

CREATE OR REPLACE FUNCTION insert_client_log()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO logs (id, time, table_name, values)
    VALUES (nextval('log_id_seq'), now(), 'client', row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_client_trigger
AFTER INSERT ON client
FOR EACH ROW
EXECUTE FUNCTION insert_client_log();

--##########################

CREATE OR REPLACE FUNCTION insert_payment_log()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO logs (id, time, table_name, values)
    VALUES (nextval('log_id_seq'), now(), 'payment', row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_payment_trigger
AFTER INSERT ON payment
FOR EACH ROW
EXECUTE FUNCTION insert_payment_log();

--###########

CREATE OR REPLACE FUNCTION insert_dish_log()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO logs (id, time, table_name, values)
    VALUES (nextval('log_id_seq'), now(), 'dish', row_to_json(NEW));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER insert_dish_trigger
AFTER INSERT ON dish
FOR EACH ROW
EXECUTE FUNCTION insert_dish_log();

--######
INSERT INTO category (category_id, name, percent, min_payment)
VALUES (2, 'silver', 0.7, 3000);

INSERT INTO client (client_id, bonus_balance, category_id)
VALUES (1, 100, 1);

INSERT INTO payment (payment_id, client_id, dish_id, dish_amount, order_id, order_time, order_sum, tips)
VALUES (1, 1, 1, 1, 1, now(), 10, 2);

INSERT INTO dish (dish_id, name, price)
VALUES (1, 'pizza', 10);

drop table category, client, dish, payment, logs;

select * 
from logs; 

INSERT INTO dish (dish_id, name, price)
VALUES (2, 'cola', 8); 


---########################


CREATE SEQUENCE mng_clients_id_seq
  START WITH 1
  INCREMENT BY 1
  NO MINVALUE
  NO MAXVALUE
  CACHE 1;

 
CREATE TABLE IF NOT EXISTS staging_mng_clients (
	id integer NOT NULL DEFAULT nextval('mng_clients_id_seq') PRIMARY KEY, 
	table_name VARCHAR(200), 
	time TIMESTAMP,
	values json
);

select * 
from staging_mng_clients;

drop table staging_mng_restaurants;
drop SEQUENCE mng_clients_id_seq;


select *
from staging_mng_restaurants;

select * 
from logs
where time > '2024-06-02 12:45:00';


select * 
from staging_pstg_category spc;


select * from staging_api_deliveryman;


---#########################################33


CREATE TABLE IF NOT EXISTS dds_category (
	category_id INT PRIMARY key,
	name VARCHAR(200),
	percent FLOAT,
	min_payment FLOAT
);


CREATE TABLE IF NOT EXISTS dds_restaurants  (
	restaurant_id  INT PRIMARY key,
	name VARCHAR(150),
	phone VARCHAR(15),
	email  VARCHAR(80),
	founding_day DATE,
	update_time TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dds_menu  (
	menu_id INT PRIMARY key,
	name VARCHAR(150),
	price FLOAT,
	dish_category VARCHAR(150)
);


CREATE TABLE IF NOT EXISTS dds_menu_restauran  (
	menu_id INT,
	restaurant_id INT,
	PRIMARY KEY (menu_id, restaurant_id), 
	FOREIGN KEY (menu_id) REFERENCES dds_menu(menu_id),
	FOREIGN KEY (restaurant_id) REFERENCES dds_restaurants(restaurant_id)
);

CREATE TABLE IF NOT EXISTS dds_diliveryman (
	deliveryman_id INT PRIMARY key,
	name VARCHAR(100),
	update_time TIMESTAMP 
);


CREATE TABLE IF NOT EXISTS dds_dilivery  (
	delivery_id INT PRIMARY key,
	order_id INT unique,
	deliveryman_id INT,
	order_date_created TIMESTAMP,
	delivery_address VARCHAR(200),
	delivery_time TIMESTAMP,
	rating INT,
	tips FLOAT,
	FOREIGN KEY (deliveryman_id) REFERENCES dds_diliveryman(deliveryman_id)
);


CREATE TABLE IF NOT EXISTS dds_dish (
	dish_id INT PRIMARY key,
	name VARCHAR(150),
	price FLOAT,
	quantity INT
); 


CREATE TABLE IF NOT EXISTS dds_clients (
	client_id INT PRIMARY key,
	category_id INT,
	name VARCHAR(200),
	phone INT,
	birthday DATE,
	email VARCHAR(200),
	login VARCHAR(200),
	address VARCHAR(400),
	bonus_balance FLOAT,
	update_time TIMESTAMP
); 

CREATE TABLE IF NOT EXISTS dds_status (
	status_id INT PRIMARY key,
	status VARCHAR(100),
	time TIMESTAMP
); 

CREATE TABLE IF NOT EXISTS dds_orders  (
	order_id INT PRIMARY key,
	restaurant_id INT,
	client_id INT,
	order_date DATE,
	payed_by_bonuses FLOAT,
	cost FLOAT,
	payment FLOAT,
	final_status VARCHAR(100),
	bonus_for_visit FLOAT,
	update_time TIMESTAMP,
	FOREIGN KEY (restaurant_id) REFERENCES dds_restaurants(restaurant_id),
	FOREIGN KEY (client_id) REFERENCES dds_clients(client_id)
);

CREATE TABLE IF NOT EXISTS dds_payment (
	payment_id INT PRIMARY key,
    client_id INT,
    order_id INT,
    dish_amount FLOAT,
    order_time TIMESTAMP,
    order_sum FLOAT,
    tips FLOAT,
	FOREIGN KEY (client_id) REFERENCES dds_clients(client_id),
	FOREIGN KEY (order_id) REFERENCES dds_orders(order_id)
);

CREATE TABLE IF NOT EXISTS dds_status_orders  (
	status_id INT,
	order_id  INT,
	PRIMARY KEY (status_id, order_id), 
	FOREIGN KEY (status_id) REFERENCES dds_status(status_id),
	FOREIGN KEY (order_id) REFERENCES dds_orders(order_id)
); 

CREATE TABLE IF NOT EXISTS dds_orders_dish (
	order_id INT,
	dish_id  INT,
	PRIMARY KEY (dish_id, order_id), 
	FOREIGN KEY (dish_id) REFERENCES dds_dish (dish_id),
	FOREIGN KEY (order_id) REFERENCES dds_orders(order_id)
); 
    
drop table if EXISTS dds_category, dds_restaurants, dds_menu, dds_menu_restauran,
		   dds_deliveryman, dds_delivery, dds_dish, dds_orders_dish, dds_clients,
		   dds_status, dds_orders, dds_payment, dds_status_orders;

		 
drop table staging_api_delivery, staging_api_deliveryman, staging_mng_clients,
			staging_mng_orders, staging_mng_restaurants,
			staging_pstg_category, staging_pstg_client,
			staging_pstg_dish, staging_pstg_payment;
		
drop table category, client, dish, logs, payment;
drop table cursor_timestamp;

select * 
from staging_api_delivery; 

CREATE TABLE IF NOT EXISTS cursor_timestamp (
	 table_name VARCHAR(100) UNIQUE,
	 cursor_time TIMESTAMP 
); 



INSERT INTO cursor_timestamp (table_name, cursor_time)
VALUES
  ('logs', '2022-01-01 00:00:00'),
  ('delivery', '2022-01-01 00:00:00'),
  ('deliveryman', '2022-01-01 00:00:00'),
  ('clients', '2022-01-01 00:00:00'),
  ('orders', '2022-01-01 00:00:00'),
  ('restaurants', '2022-01-01 00:00:00'),
  ('staging_api_delivery', '2022-01-01 00:00:00'),
  ('staging_api_deliveryman', '2022-01-01 00:00:00'),
  ('staging_mng_clients', '2022-01-01 00:00:00'),
  ('staging_mng_orders', '2022-01-01 00:00:00'),
  ('staging_mng_restaurants', '2022-01-01 00:00:00'),
  ('staging_pstg_category', '2022-01-01 00:00:00'),
  ('staging_pstg_client', '2022-01-01 00:00:00'),
  ('staging_pstg_dish', '2022-01-01 00:00:00'),
  ('staging_pstg_payment', '2022-01-01 00:00:00');
 
 
select * 
from staging_api_delivery;
 
SELECT time 
FROM logs
WHERE time = (SELECT max(time) FROM logs);

DROP SCHEMA public CASCADE;
CREATE SCHEMA public; 

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT sequencename FROM pg_sequences WHERE schemaname = 'public') LOOP
        EXECUTE 'DROP SEQUENCE ' || quote_ident(r.sequencename) || ';';
    END LOOP;
END $$;


DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')) LOOP
        EXECUTE 'DROP FUNCTION ' || quote_ident(r.proname) || '();';
    END LOOP;
END $$;




CREATE TABLE IF NOT EXISTS dds_category (
	category_id INT PRIMARY key,
	name VARCHAR(200),
	percent FLOAT,
	min_payment FLOAT
);


CREATE TABLE IF NOT EXISTS dds_restaurants  (
	restaurant_id  INT PRIMARY key,
	name VARCHAR(150),
	phone VARCHAR(15),
	email  VARCHAR(80),
	founding_day DATE,
	update_time TIMESTAMP
);


CREATE TABLE IF NOT EXISTS dds_menu  (
	menu_id INT PRIMARY key,
	name VARCHAR(150),
	price FLOAT,
	dish_category VARCHAR(150)
);


CREATE TABLE IF NOT EXISTS dds_menu_restauran  (
	menu_id INT,
	restaurant_id INT,
	PRIMARY KEY (menu_id, restaurant_id)
);

CREATE TABLE IF NOT EXISTS dds_deliveryman (
	deliveryman_id INT PRIMARY key,
	name VARCHAR(100),
	update_time TIMESTAMP 
);


CREATE TABLE IF NOT EXISTS dds_delivery  (
	delivery_id INT PRIMARY key,
	order_id INT unique,
	deliveryman_id INT,
	order_date_created TIMESTAMP,
	delivery_address VARCHAR(200),
	delivery_time TIMESTAMP,
	rating INT,
	tips FLOAT
);

CREATE TABLE IF NOT EXISTS dds_dish (
	dish_id INT PRIMARY key,
	name VARCHAR(150),
	price FLOAT,
	quantity INT
); 

CREATE TABLE IF NOT EXISTS dds_clients (
	client_id INT PRIMARY key,
	category_id INT,
	name VARCHAR(200),
	phone INT,
	birthday DATE,
	email VARCHAR(200),
	login VARCHAR(200),
	address VARCHAR(400),
	bonus_balance FLOAT,
	update_time TIMESTAMP
); 

CREATE TABLE IF NOT EXISTS dds_status (
	status_id INT PRIMARY key,
	status VARCHAR(100),
	time TIMESTAMP
); 


CREATE TABLE IF NOT EXISTS dds_orders  (
	order_id INT PRIMARY key,
	restaurant_id INT,
	client_id INT,
	order_date TIMESTAMP,
	payed_by_bonuses FLOAT,
	cost FLOAT,
	payment FLOAT,
	bonus_for_visit FLOAT,
	final_status VARCHAR(100),
	update_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dds_payment (
	payment_id INT PRIMARY key,
    client_id INT,
    order_id INT,
    dish_amount FLOAT,
    order_time TIMESTAMP,
    order_sum FLOAT,
    tips FLOAT
);

CREATE TABLE IF NOT EXISTS dds_status_orders  (
	status_id INT,
	order_id  INT,
	PRIMARY KEY (status_id, order_id)
); 

CREATE TABLE IF NOT EXISTS dds_orders_dish (
	order_id INT,
	dish_id  INT,
	PRIMARY KEY (dish_id, order_id)
); 


--################################################################################################################

-- вставляем данные в таблицу client
INSERT INTO client (client_id, bonus_balance, category_id)
VALUES
  (1, 100.0, 1),
  (2, 50.0, 2),
  (3, 0.0, 3),
  (4, 150.0, 1),
  (5, 70, 2),
  (6, 22, 3),
  (7, 26, 1),
  (8, 378, 3),
  (9, 10, 1);

-- вставляем данные в таблицу category
INSERT INTO category (category_id, name, percent, min_payment)
VALUES
  (1, 'Silver', 0.05, 100.0),
  (2, 'Gold', 0.1, 500.0),
  (3, 'Platinum', 0.15, 1000.0);

-- вставляем данные в таблицу payment
INSERT INTO payment (payment_id, client_id, dish_id, dish_amount, order_id, order_time, order_sum, tips)
VALUES
  (1, 1, 1, 2.0, 1, '2024-01-01 12:00:00', 50.0, 5.0),
  (2, 2, 2, 1.0, 2, '2024-01-01 13:00:00', 20.0, 2.0),
  (3, 3, 3, 3.0, 3, '2024-01-01 14:00:00', 150.0, 15.0);

-- вставляем данные в таблицу dish
INSERT INTO dish (dish_id, name, price)
VALUES
  (1, 'Pizza', 25.0),
  (2, 'Burger', 10.0),
  (3, 'Steak', 50.0);
 
 
-- *, EXTRACT(MONTH FROM date(order_date)) as ord_month,



deliveryman_order_income — перечисляемые курьеру за заказ, 
если rating < 8, то стоимость_заказа*0,05 (но должно начислиться не меньше 400р),
если rating >=10 , то стоимость_заказа*0,1 (но должно начислиться не больше 1000р)


with flat_table as (
	select 	dm.deliveryman_id, name, d.delivery_id, o.order_id, rating,
			tips, o.client_id, cost, EXTRACT(MONTH FROM date(order_date)) as ord_month,
	CASE
	        WHEN rating < 8 THEN GREATEST(cost  * 0.05, 400) 
	        WHEN rating >= 10 THEN LEAST(cost * 0.1, 1000)
	        ELSE NULL
	    end as deliveryman_order_income 
	from dds_deliveryman as dm 
	join dds_delivery as d on dm.deliveryman_id = d.deliveryman_id 
	join dds_orders as o on o.order_id = d.order_id
	)


select  deliveryman_id, name, ord_month, count(delivery_id) as orders_amount, sum (cost) as orders_total_cost, 
		cast(AVG(rating) as FLOAT) as avg_rating, sum(cost) * 0.5  as company_commission,
		SUM(deliveryman_order_income) as sum_income, sum(tips) as tips
from flat_table
group by deliveryman_id, name, ord_month
order by deliveryman_id;


select * from dm_delivery_calculation;





