drop table if exists STV2023121131__DWH.l_user_group_activity;

create table STV2023121131__DWH.l_user_group_activity(
	hk_l_user_group_activity bigint primary key,
	hk_user_id bigint not null CONSTRAINT fk_l_user_message_user REFERENCES STV2023121131__DWH.h_users (hk_user_id),
	hk_group_id  bigint not null CONSTRAINT fk_l_groups_dialogs_group REFERENCES STV2023121131__DWH.h_groups (hk_group_id),
	load_dt datetime,
	load_src varchar(20) 
);