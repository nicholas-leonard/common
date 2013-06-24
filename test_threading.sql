/*
 * test_threadning.sql
 * 
 * Copyright 2012 Nicholas Leonard <nick@nikopia.org>
 * 
 */

CREATE SCHEMA test;

--DROP TABLE test.process;
CREATE TABLE test.process (
	process_type	VARCHAR(255),
	process_key		INT4,
	listen_address	VARCHAR(255),
	listen_port		INT2,
	PRIMARY KEY (process_type, process_key)
);

--DROP TABLE test.actor_process;
CREATE TABLE test.actor_process (
	actor_type	VARCHAR(255),
	actor_key	INT4,
	process_type	VARCHAR(255),
	process_key	INT4,
	PRIMARY KEY (actor_type, actor_key)
);
CREATE INDEX actor_process_id ON test.actor_process (process_type, process_key);

INSERT INTO test.process (process_type, process_key, listen_address, listen_port) VALUES
	('Server', 0, 'localhost', 2222),
	('Client', 0, 'localhost', 2223);

INSERT INTO test.actor_process (actor_type, actor_key, process_type, process_key) (
	SELECT 'Print', generate_series(0, 3), 'Server', 0
);

INSERT INTO test.actor_process (actor_type, actor_key, process_type, process_key) (
	SELECT 'Forward', generate_series(0, 7), 'Client', 0
);

INSERT INTO test.actor_process (actor_type, actor_key, process_type, process_key) (
	SELECT 'Distribute', generate_series(0, 2), 'Client', 0
);

INSERT INTO test.actor_process (actor_type, actor_key, process_type, process_key) VALUES 
	('Server', 0, 'Server', 0),
	('Client', 0, 'Client', 0);
