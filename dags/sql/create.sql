-- Tabela de Campanhas

create table campaigns (
    adset_id bigint PRIMARY KEY,
    adset_name varchar(255) NOT NULL,
    campaign_id bigint NOT NULL,
    campaign_name varchar(255) NOT NULL,
    spend decimal(10,5),
    objective varchar(50),
    date_start date,
    date_stop date,
    status varchar(50) NOT NULL,
    created_time datetime,
	leads int
);

-- Manual Create Table