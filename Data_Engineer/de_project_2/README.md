## Проект:

Создание новой модели данных и формирование витрины для аналитики

## Задача:

Осуществить миграцию в отдельные логические таблицы, а затем собрать на них витрину данных

## Решение:

- Создана пустая таблица-справочник shipping_country_rates
- Создан справочник тарифов доставки вендора по договору shipping_agreement
- Создан справочник типов доставки shipping_transfer
- Создан справочник комиссии по странам shipping_info
- Создана таблицу статусов доставки:
  - заполнена таблицу shipping_country_rates
  - заполнена таблицу shipping_agreement
  - заполнена таблицу shipping_transfer
  - заполнена таблицу shipping_info
  - заполнена таблицу shipping_status
  - заполнена временную таблицу агрегированными данными по shipping_id, shipping_start_fact_datetime и shipping_end_fact_datetime
  - заполнена итоговую таблицу объединением таблиц shipping и shipping_status_temp
  - удалена временная таблица shipping_status_temp
- Создано представление shipping_datamart
