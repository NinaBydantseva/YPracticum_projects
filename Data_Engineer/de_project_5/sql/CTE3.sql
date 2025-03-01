with user_group_log as (
    select
		lu.hk_group_id,
		count(distinct(lu.hk_user_id)) as cnt_added_users
	from STV2023121131__DWH.l_user_group_activity lu
	join STV2023121131__DWH.s_auth_history sah on lu.hk_l_user_group_activity=sah.hk_l_user_group_activity 
	join (select * from STV2023121131__DWH.h_groups hg order by hg.registration_dt asc limit 10) as foo2 on foo2.hk_group_id=lu.hk_group_id 
	WHERE sah.event='add'
	group by lu.hk_group_id)
,user_group_messages as (
    select
    	lu.hk_group_id as hk_group_id,
    	count(distinct(lu.hk_user_id)) as cnt_users_in_group_with_messages
    from STV2023121131__DWH.l_user_group_activity lu
    inner join (select distinct lum.hk_user_id, lum.hk_message_id from STV2023121131__DWH.l_user_message lum) as foo on lu.hk_user_id =foo.hk_user_id 
    group by lu.hk_group_id)
select
	ugl.hk_group_id,
	ugl.cnt_added_users,
	ugm.cnt_users_in_group_with_messages,
	(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc