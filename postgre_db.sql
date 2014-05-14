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

create index idx_offer_offer on raw.offer(offer);

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

create table model.trans1 as (
	select * from raw.transactions
	where dept in (
		select category/100 from raw.offer
	)
);

create index idx_trans1_id_chain on model.trans1(id, chain);

create table model.trans as (
	select * from model.trans1
	where (id, chain) in (
		select id, chain from raw.trainHistory
	)
);

analyze model.trans;

create index idx_model_trans_id_chain_dept on model.trans(id, chain, dept);

select count(1) from model.trans;
--45,279,648


create table model.trainHist_with_offer as (
	select 
		h.*,
		o.category/100 as offer_dept,
		o.category as offer_category,
		o.quantity as offer_quantity,
		o.company as offer_company,
		o.offervalue as offer_value,
		o.brand as offer_brand
	from raw.trainHistory h
	inner join raw.offer o
	on h.offer = o.offer
);

create index idx_model_trainHist_with_offer_id on model.trainHist_with_offer(id, chain, offer_dept);

create table model.train as (
	select 
		t.*,
		h.offer,
		h.market,
		h.repeattrips,
		h.repeater,
		h.offerdate,
		h.offer_dept,
		h.offer_category,
		h.offer_quantity,
		h.offer_company,
		h.offer_brand,
		h.offer_value,
		h.offerdate - t.date as n_days_before
	from model.trainHist_with_offer h
	inner join model.trans t
	on h.id = t.id
	and h.chain = t.chain
	and h.offer_dept = t.dept
);


select 
	id, 
	chain,
	case when category = offer_category then 1
		else 0 end has_bought_category,
	case when company = offer_company then 1
		else 0 end has_bought_company,
	case when brand = offer_brand then 1
		else 0 end has_bought_brand
	
from model.train
where n_days_before <= 30
and id = 86246
;
