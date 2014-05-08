\connect kaggle_valued_shoppers

create schema raw;

drop table raw.transactions;

create table raw.transactions (
	id bigint,
	chain int,
	dept int,
	category int,
	company bigint,
	brand bigint,
	date date,
	productsize decimal,
	productmeasure varchar(20),
	purchasequantity int,
	purchaseamount decimal
);

copy raw.transactions from 'C:\speed\transactions.csv'
with (format csv, header true);
--Query returned successfully: 349,655,789 rows affected, 3940.472 ms execution time.

analyze raw.transactions;

create index idx_transactions_id on raw.transactions(id);
--Query returned successfully with no result in 1459849 ms.

create index idx_transactions_id_chain on raw.transactions(id, chain);
create index idx_transactions_dept on raw.transactions(dept);

create table raw.trainHistory (
	id bigint,
	chain int,
	offer int,
	market int,
	repeattrips int,
	repeater varchar(1),
	offerdate date
);

copy raw.trainHistory from 'C:\speed\trainHistory.csv'
with (format csv, header true);
--Query returned successfully: 160057 rows affected, 1341 ms execution time.

create index idx_trainHistory_id on raw.trainHistory(id);
create index idx_trainHistory_id_chain on raw.trainHistory(id, chain);
create index idx_trainHistory_offer on raw.trainHistory(offer);
create index idx_trainHistory_offerdate on raw.trainHistory(offerdate);

create table raw.offer (
	offer int,
	category int,
	quantity int,
	company bigint,
	offervalue decimal,
	brand int
);

copy raw.offer from 'C:\speed\offers.csv'
with (format csv, header true);

create table raw.testHistory (
	id bigint,
	chain int,
	offer int,
	market int,
	offerdate date
);

copy raw.testHistory from 'C:\speed\testHistory.csv'
with (format csv, header true);

create index idx_testHistory_id on raw.testHistory(id);
create index idx_testHistory_id_chain on raw.testHistory(id, chain);

select count(distinct id) from raw.transactions;
--311,541


-- Data reduction
create schema model
;

create table model.trans as (
	select * from raw.transactions
	where dept in (
		select category/100 from raw.offer
	)
	and (id, chain) in (
		select id, chain from raw.trainHistory
	)
);

