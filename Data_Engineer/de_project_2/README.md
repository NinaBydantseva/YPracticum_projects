## Проект:

Создание модели данных и формирование витрины для аналитики доставки товаров

## Задача:

Осуществить миграцию в отдельные логические таблицы, а затем собрать на них витрину данных

## Решение:

- Создана пустая таблица-справочник shipping_country_rates
- Создан справочник тарифов доставки вендора по договору shipping_agreement
- Создан справочник типов доставки shipping_transfer
- Создан справочник комиссии по странам shipping_info
- Создана таблица статусов доставки:
  - заполнена таблица shipping_country_rates
  - заполнена таблица shipping_agreement
  - заполнена таблица shipping_transfer
  - заполнена таблица shipping_info
  - заполнена таблица shipping_status
  - заполнена временная таблица агрегированными данными по shipping_id, shipping_start_fact_datetime и shipping_end_fact_datetime
  - заполнена итоговая таблица объединением таблиц shipping и shipping_status_temp
  - удалена временная таблица shipping_status_temp
- Создано представление shipping_datamart
