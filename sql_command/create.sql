CREATE TABLE inventory (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	quantity INT,
	PRIMARY KEY(id)
);

CREATE TABLE product (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	Year VARCHAR(4),
	Make VARCHAR(40),
	Model VARCHAR(40),
	Category VARCHAR(40),
	inventory_id INT,
	created_at VARCHAR(20),
	PRIMARY KEY(id)
);

CREATE TABLE orders (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	product_id INT,
	quantity INT,
	created_at VARCHAR(20),
	PRIMARY KEY(id)
);

CREATE TABLE order_detail (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	order_id INT,
	user_id INT,
	total INT,
	payment VARCHAR(20),
	PRIMARY KEY(id)
);

CREATE TABLE user (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	username VARCHAR(40),
	firstname VARCHAR(20),
	lastname VARCHAR(20),
	email VARCHAR(40),
	PRIMARY KEY(id)
);

CREATE TABLE user_details (
	id INT UNSIGNED NOT NULL AUTO_INCREMENT,
	user_id INT,
	address VARCHAR(200),
	city VARCHAR(100),
	postcode INT,
	country VARCHAR(100),
	PRIMARY KEY(id)
);