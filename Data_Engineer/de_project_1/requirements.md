**Требования к витрине.**
Что сделать: построить витрину для RFM-классификации. Для анализа нужно отобрать только успешно выполненные заказы (это заказ со статусом Closed). Витрину нужно назвать dm_rfm_segments. Сохранить в схему analysis.
Зачем: подготовить данные для команды специалистов по Data Science. Им нужно построить модель, которая кластеризует пользователей на основании их действий. 
Ограничение по датам: с начала 2022 года.
Обновление данных: не нужны.
Кому доступна: не указано, поэтому примем, что всем с доступом только на чтение.
Исходные данные: В схеме production.
Сегментирование: количество клиентов в каждом сегменте одинаково. 5 значений от 1 до 5 по возрастанию итоговых значений параметров RFM.
recency: значение 1 получают те, кто либо вообще не делал заказов, либо делал их очень давно, а 5 — те, кто заказывал относительно недавно,
frequency: значение 1 получают клиенты с минимальным количеством заказов, а 5 — с наибольшим,
monetary_value: значение 1 получают клиенты с наименьшей суммой, а 5 — с наибольшей.
Необходимая структура:
user_id
recency (число от 1 до 5) 
frequency (число от 1 до 5)  
monetary_value (число от 1 до 5)
**Изучение структуры данных.**
В схеме analysis таблицы отсутствуют.
В схеме production есть таблицы:
**orderitems**
orderitems.id int4
orderitems.product_id int4
orderitems.order_id int4
orderitems.name varchar(2048)
orderitems.price numeric (19,5)
orderitems.discount numeric (19,5)
orderitems.quantity int4
**orders**
orders.order_id int4
orders.order_ts timestamp
orders.user_id int4
orders.bonus_payment numeric (19,5)
orders.payment numeric (19,5)
orders.cost numeric (19,5)
orders.bonus_grant numeric (19,5)
orders.status int4
**orderstatuses**
orderstatuses.id int4
orderstatuses.key varchar(255)
**orderstatuslog**
orderstatuslog.id int4
orderstatuslog.order_id int4
orderstatuslog.status_id int4
orderstatuslog.dttm timestamp
**products**
products.id int4
products.name varchar(2048)
products.price numeric (19,5)
**users**
users.id int4
users.name varchar(2048)
users.login varchar(2048)
Для формирования метрик нам понадобятся таблицы **orderstatuslog** и **orderstatuses**, чтобы ограничить выборку заказами в поле orderstatuses.key='Closed' и
для R нам понадобятся поля user_id, order_id и order_ts таблицы orders,
для F нам понадобятся поля user_id, order_id таблицы orders,
для M нам понадобятся поля user_id, order_id, payment таблицы orders.