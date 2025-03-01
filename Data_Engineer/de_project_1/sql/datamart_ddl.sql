create table analysis.dm_rfm_segments (
user_id INT not null primary key,
recency int not null check (recency>=1 and recency<=5),
frequency int not null check (frequency>=1 and frequency<=5),
monetary_value int not null check (monetary_value>=1 and monetary_value<=5)
);