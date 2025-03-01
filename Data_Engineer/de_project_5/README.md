## Проект:

Работа с аналитической базой данных Vertica

## Задача:

Вычислить конверсионные показатели для десяти самых старых групп в соцсети, чтобы помочь маркетологам дать эффективную рекламу этой соцсети в интернете. 

## Решение:
- Используемые библиотеки Python:
  - boto3,
  - pendulum,
  - contextlib,
  - hashlib,
  - json,
  - typing,
  - airflow,
  - pandas,
  - vertica_python.

### Ход решения:
- создана функция загрузки бакетов с сервера s3 - fetch_s3_file,
- создан пайплайн DAG для загрузки данных project6_dag_get_data, fetch_tasks >> print_10_lines_of_file,
- создана таблица group_log в Vertica:
- создана функция загрузки полученных данных в Vertica - load_dataset_file_to_vertica, start >> load_group_log>> end,
- создан пайплайн DAG для загрузки данных в Vertica в таблицу group_log слоя stg - project6_dag_load_data_to_staging,
- создана таблица связи l_user_group_activity,
- загружены данные в таблицу связи l_user_group_activity,
- создана таблица сателлитов s_auth_history,
- загружены данные в таблицу сателлитов s_auth_history,
- создана временная таблица, из которой можно получить количество уникальных пользователей в группе, которые написали хотя бы раз - user_group_messages,
- создана временная таблица, из которой можно получить количество пользователей, вступивших в группы - user_group_log,
- рассчитаны конверсионные показатели для десяти самых старых групп - group_conversion.
