with user_group_messages as (
    select
    	lu.hk_group_id as hk_group_id,
    	count(distinct(lu.hk_user_id)) as cnt_users_in_group_with_messages
    from STV2023121131__DWH.l_user_group_activity lu
    inner join (select distinct lum.hk_user_id, lum.hk_message_id from STV2023121131__DWH.l_user_message lum) as foo on lu.hk_user_id =foo.hk_user_id 
    group by lu.hk_group_id
)
select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10;