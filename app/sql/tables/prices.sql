create table prices(
	id serial not null primary key,
	token_id varchar(64) not null,
	price decimal(20, 10) default 0.0
)