-- ----- Creamos la bsae de datos. 
CREATE DATABASE tienda_online;
-- ----- Una vez creada empezamos a crear las tablas y a cagar los datos en ellas. 
USE tienda_online;
-- ------------------------------------------
-- ----- Creamos la tabla transactions
-- ------------------------------------------
CREATE TABLE transactions (
	id	VARCHAR(100) PRIMARY KEY NOT NULL,
    card_id	VARCHAR(50) NULL,
    business_id	VARCHAR(50) NULL,
    `timestamp` TIMESTAMP NULL,
    amount DECIMAL(10,2) NULL,
    declined TINYINT(1) NOT NULL,
    product_ids	VARCHAR(100) NULL,
    user_id	VARCHAR(100) NULL,
    lat	 VARCHAR(100) NULL,
    longitude VARCHAR(100) NULL
);
-- ----- Cargamos los datos de transactions y comprobamos.
LOAD DATA 
INFILE "C://transactions.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- -----
SELECT * 
FROM transactions; 

-- ------------------------------------------
-- ----- Creamos la tabla companies
-- ------------------------------------------
CREATE TABLE companies (
	company_id VARCHAR(100) PRIMARY KEY NOT NULL,
    company_name VARCHAR(50) NULL,
    phone VARCHAR(50) NULL,
    email VARCHAR(50) NULL,
    country VARCHAR(50) NULL,
    website VARCHAR(50) NULL
);
-- ----- Cargamos los datos de companies y comprobamos. 
LOAD DATA 
INFILE "C://companies.csv"
INTO TABLE companies
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- -----
SELECT * 
FROM companies;

-- ------------------------------------------
-- ----- Creamos la tabla credit_cards
-- ------------------------------------------
CREATE TABLE credit_cards (
	id VARCHAR(100) PRIMARY KEY NOT NULL,
    user_id VARCHAR(50) NULL,
    iban VARCHAR(100) NULL,
    pan VARCHAR(100) NULL,
    pin VARCHAR(50) NULL,
    cvv VARCHAR(50) NULL,
    track1 VARCHAR(250) NULL,
    track2 VARCHAR(250) NULL,
    expiring_date VARCHAR(50) NULL
);
-- ----- Cargamos los datos de credit_cards y comprobamos. 
LOAD DATA 
INFILE "C://credit_cards.csv"
INTO TABLE credit_cards
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- -----
SELECT * 
FROM credit_cards;

-- ------------------------------------------
-- ----- Creamos la tabla users
-- ------------------------------------------
CREATE TABLE users (
	id VARCHAR(100) PRIMARY KEY NOT NULL,
    `name` VARCHAR(100) NULL,
    surname VARCHAR(100) NULL,
    phone VARCHAR(100) NULL,
    email VARCHAR(100) NULL,
    birth_date VARCHAR(50) NULL,
    country VARCHAR(50) NULL,
    city VARCHAR(50) NULL,
    postal_code VARCHAR(100) NULL,
    address VARCHAR(150) NULL
);
-- ----- Cargamos los datos de american_users. 
LOAD DATA 
INFILE "C://american_users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- ----- Cargamos los datos de european_users y comprobamos. 
LOAD DATA 
INFILE "C://european_users.csv"
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- -----  
SELECT * 
FROM users; 

-- ------------------------------------------
-- ----- Creamos las FK's para relacionar las tablas 
-- ------------------------------------------
ALTER TABLE transactions
	ADD CONSTRAINT  FK_business_id
	FOREIGN KEY (business_id) REFERENCES companies(company_id); 
-- -----
ALTER TABLE transactions        
    ADD CONSTRAINT  FK_card_id
	FOREIGN KEY (card_id) REFERENCES credit_cards(id);
-- -----
ALTER TABLE transactions
	ADD CONSTRAINT  FK_user_id
	FOREIGN KEY (user_id) REFERENCES users(id);

-- ------------------------------------------------------------------------------------------------------------------------------
-- N1.1 Realizamos una subconsulta que muestra a todos los usuarios con más de 80 transacciones.
-- ------------------------------------------------------------------------------------------------------------------------------
SELECT 
	u.id,
    CONCAT(u.`name`," ", u.surname) AS nombre_completo
FROM users u 
WHERE EXISTS( SELECT COUNT(t.id) AS num_transacciones
			  FROM transactions t
              WHERE t.user_id = u.id
              AND t.declined = 0
              HAVING num_transacciones > 80
              )
;

-- ------------------------------------------------------------------------------------------------------------------------------
-- N1.2 Mostramos la media de amount por IBAN de las tarjetas de crédito en la compañía "Donec Ltd".
-- ------------------------------------------------------------------------------------------------------------------------------
SELECT 
	cc.iban,
    ROUND(AVG(t.amount) ,2) AS media_monto
FROM credit_cards cc 
JOIN transactions t 
	ON cc.id = t.card_id
JOIN companies c
	ON t.business_id = c.company_id
WHERE c.company_name = 'Donec Ltd'
AND t.declined = 0
GROUP BY cc.iban
ORDER BY media_monto DESC;

-- ------------------------------------------------------------------------------------------------------------------------------
-- N2. Creamos una nueva tabla que refleja el estado de las tarjetas de crédito basado en si las tres últimas transacciones han sido declinadas entonces es inactivo, 
-- --- si al menos una no es rechazada entonces es activo. Despues mostramos cuantas están activas.
-- ------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE estado_de_tarjeta (
    id_tarjeta VARCHAR(100) NOT NULL PRIMARY KEY,
    estado VARCHAR(50) NOT NULL,
    FOREIGN KEY (id_tarjeta) REFERENCES credit_cards(id)
);
-- ----- Insertamos los datos filtrados.
INSERT INTO estado_de_tarjeta (id_tarjeta, estado)
WITH crs AS (SELECT
				t.card_id,
				t.declined,
				ROW_NUMBER() OVER(PARTITION BY t.card_id ORDER BY t.`timestamp` DESC) AS num_transaccion
			FROM transactions t)
SELECT
    crs.card_id AS id_tarjeta,
    CASE
        WHEN SUM(crs.declined) = 3 THEN 'INACTIVA'
        ELSE 'ACTIVA'
    END AS estado
FROM crs
WHERE num_transaccion <= 3
GROUP BY id_tarjeta;

-- ----- Vemos la tabla.
SELECT * 
FROM estado_de_tarjeta;
-- ----- Vemos cuantas tarjestas están activas. 
SELECT 
	COUNT(edt.id_tarjeta) AS num_tarjetas_activas
FROM estado_de_tarjeta edt 
WHERE edt.estado = 'ACTIVA'; 

-- ------------------------------------------------------------------------------------------------------------------------------
-- N3. Creamos una tabla con la que podemos unir los datos del nuevo archivo products.csv con la base de datos creada, teniendo en cuenta que 
-- --- desde transaction tienes product_ids. Despues vemos el número de veces que se ha vendido cada producto.
-- ------------------------------------------------------------------------------------------------------------------------------
-- ----- Creamos la tabla products
CREATE TABLE products (
	id VARCHAR(100) PRIMARY KEY NOT NULL,
    product_name VARCHAR(100) NULL,
    price VARCHAR(50) NULL,
    colour VARCHAR(100) NULL,
    weight DECIMAL(10,2) NULL,
    warehouse_id VARCHAR(100) NULL	
);
-- ----- Cargamos los datos de products y comprobamos. 
LOAD DATA 
INFILE "C://products.csv"
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
-- -----  
SELECT * 
FROM products; 
-- ----- Creamos la tabala intermedia transaction_product. 
CREATE TABLE transactions_products (
	id_transaction VARCHAR(100) NOT NULL,
    id_product VARCHAR(100) NOT NULL,
    FOREIGN KEY (id_transaction) REFERENCES transactions(id),
    FOREIGN KEY (id_product) REFERENCES products(id) )
;
-- ----- extraemos los datos de la columna "transactions.product_ids" e insetamos en la tabala inermedia "transactions_products".
INSERT INTO transactions_products (id_transaction, id_product)
WITH temporal_table AS (  	SELECT 
								t.id AS id_transaction,
                                ep.id_product
							FROM transactions t,
							JSON_TABLE (	CONCAT('[',t.product_ids,']'),
											'$[*]'
											COLUMNS (
												id_product INT PATH '$')
										) 	AS ep )
SELECT *
FROM temporal_table;

-- ----- Vemos la tabla "transactions_products".
SELECT *
FROM transactions_products;
-- ----- Vemos el numero de veces que se ha vendido cada producto. 
SELECT 
	p.product_name AS nombre_producto,
    tp.id_product AS id_producto,
	COUNT( tp.id_product) AS num_ventas
FROM transactions_products tp
JOIN transactions t
	ON tp.id_transaction = t.id
JOIN products p 
	ON tp.id_product = p.id
WHERE t.declined = 0
GROUP BY nombre_producto,
		 id_producto 
ORDER BY num_ventas DESC;

