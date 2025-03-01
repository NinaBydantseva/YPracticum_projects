with user_group_log as (
    select
		lu.hk_group_id,
		count(distinct(lu.hk_user_id)) as cnt_added_users
	from STV2023121131__DWH.l_user_group_activity lu
	join STV2023121131__DWH.s_auth_history sah on lu.hk_l_user_group_activity=sah.hk_l_user_group_activity 
	join (select * from STV2023121131__DWH.h_groups hg order by hg.registration_dt asc limit 10) as foo2 on foo2.hk_group_id=lu.hk_group_id 
	WHERE sah.event='add'
	group by lu.hk_group_id)
select hk_group_id
            ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;