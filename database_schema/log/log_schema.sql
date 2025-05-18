CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE public.etl_log (
	log_id uuid DEFAULT uuid_generate_v4() NOT NULL,
	step varchar NULL,
	component varchar NULL,
	status varchar NULL,
	table_name varchar NULL,
	etl_date timestamp NOT NULL,
	error_msg varchar NULL,
	CONSTRAINT etl_log_tmp_pk PRIMARY KEY (log_id)
);