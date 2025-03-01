## Проект:

Создание новой модели данных и формирование витрины для аналитики

## Задача:



## Решение:

- Создаем пустую таблицу-справочник shipping_country_rates
- Создаем справочник тарифов доставки вендора по договору shipping_agreement
- Создаем справочник типов доставки shipping_transfer
- Создаем справочник комиссии по странам shipping_info
- Создаем таблицу статусов доставки:
  - Заполняем таблицу shipping_country_rates
  - Заполняем таблицу shipping_agreement
  - Заполняем таблицу shipping_transfer
  - Заполняем таблицу shipping_info
  - Заполняем таблицу shipping_status
  - Заполняем временную таблицу агрегированными данными по shipping_id, shipping_start_fact_datetime и shipping_end_fact_datetime
  - Заполняем итоговую таблицу объединением таблиц shipping и shipping_status_temp
  - Удаляем временную таблицу shipping_status_temp
- Создаем представление shipping_datamart
- Проверяем, что представление создано
