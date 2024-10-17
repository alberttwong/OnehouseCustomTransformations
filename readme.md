Onehouse.ai Custom Transformation based on https://docs.onehouse.ai/docs/product/ingest-data/transformations/custom-transformers

Postgressql setup
```
-- global changes
CREATE USER cdc_user WITH ENCRYPTED PASSWORD 'abcd1234';
GRANT rds_superuser TO cdc_user;

-- schema specific changes
GRANT ALL ON SCHEMA public TO cdc_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cdc_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO cdc_user;

-- database specific change
CREATE PUBLICATION alltables FOR ALL TABLES;

-- database specific changes
CREATE TABLE onehouse_heartbeat (
    id INTEGER DEFAULT 1 PRIMARY KEY, 
    updated_at timestamp
    );

INSERT INTO onehouse_heartbeat DEFAULT VALUES ON CONFLICT (id) DO UPDATE SET updated_at=NOW();

CREATE TABLE toys (
    toy_id INTEGER PRIMARY KEY,
    name TEXT,
    price INTEGER
);

INSERT INTO toys(toy_id, name, price)
VALUES
    ('101', 'Jobs', '2000'),
    ('102', 'Geeta', '250'),
    ('103', 'Ramayana', '354'),
    ('104', 'Vedas', '268');   

INSERT INTO toys(toy_id, name, price)
VALUES
    ('202', 'Jobs', '2000');
   
INSERT INTO toys(toy_id, name, price)
VALUES
    ('203', 'Jobs', '2000');
```
