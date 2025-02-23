## Проект:

Определение стоимости автомобиля

## Задача:

Необходимо **построить модель для определения стоимости автомобиля** на основе исторических данных: технические характеристики, комплектации и цены автомобилей для разработки приложения для привлечения новых клиентов, которые смогут быстро узнать в нем рыночную стоимость своего автомобиля. 
Для нас важны:
- качество предсказания;
- скорость предсказания;
- время обучения.

## Решение:
- Используемые библиотеки Python:
  - pandas,
  - numpy,
  - seaborn,
  - phik,
  - matplotlib.pyplot,
  - sklearn,
  - catboost,
  - lightgbm.

### Ход решения:

- Проведен анализ предоставленных заказчиком  данных с характеристиками машин и их стоимостью.     
- В ходе предобработки и исследования представленных данных проведена их нормализация:
    - удалены дубли,
    - удалены аномальные значения,
    - пропуски временно до обучения заменены на 'unk'.
- Матрицей корреляции определены признаки, влияющие на целевой признак. На основании матрицы удалены признаки, которые не влияют на результат, а значит только бы мешали обучению и занимали нужную нам память процессора.
- Выбраны 4 модели для предсказания стоимости автомобиля:
    - LinearRegression,
    - DecisionTreeRegressor,
    - CatBoostRegressor,
    - LightGBM.
- Собран пайплайн, в котором заполняются пропуски по часто встречающемуся значению в колонке, кодируются и масштабируются признаки.
- Подобраны гиперпараметры для выбранных моделей.
- В качестве покзателя точности построения модели выбрана ошибка RMSE, которая определялась на кросс-валидации.
- Зафиксировано время обучения моделей и построена промежуточная таблица сравнения итоговых показателей построенных моделей.
- По результатам сравнения выбрана модель градиентного спуска CatBoostRegressor, чьи прогнозы оказались ближе всех к истинным, именно поэтому время (максимальное из всех моделей), которая модель затратила на поиск решения, мы сочли приемлемым
- Проведен анализ выбранной модели на тестовой выборке, построен график сравнения предсказаний и истинных значений.
- Сформирована итоговая таблица с основными параметрами выбранной модели CatBoostRegressor.

## Вывод:

Поставленная задача решена: мы построили модель машинного обучения по требованиям заказчика.
Основными параметрами, которые влияют на конечную стоимость автомобиля, оказались:
  - была ли машина в ремонте,
  - пробег,
  - мощность двигателя.

Ошибка предсказания стоимости авто составила 1700 евро.
