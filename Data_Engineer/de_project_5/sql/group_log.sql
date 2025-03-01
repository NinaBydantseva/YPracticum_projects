drop table if exists STV2023121131__STAGING.group_log;

create table STV2023121131__STAGING.group_log (
	group_id int REFERENCES groups(id) NOT NULL,
	user_id int REFERENCES users(id) NOT NULL,
	user_id_from int REFERENCES users(id),
	event varchar(10) CONSTRAINT event_check CHECK (event='create' or event='add' or event='leave'),
	"datetime" datetime NOT NULL
)
ORDER BY group_id, user_id
SEGMENTED BY hash(group_id, user_id) all nodes
PARTITION BY "datetime"::date
GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);