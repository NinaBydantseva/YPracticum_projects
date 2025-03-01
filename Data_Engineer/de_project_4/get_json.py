import requests

needs=['restaurants','deliveries','couriers']

headers={'X-Nickname': 'NinaBydantseva','X-Cohort': '19','X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
for i in needs:
    url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{i}/?sort_field=order_ts&sort_direction=asc&&offset=0"
    response=requests.get(url, headers=headers)
    with open(f"d:\\WORK\\2023\\Learning\\sprint5\\!!!PROJECT_sprint5\\{i}.txt", "w") as f:
        f.write(response.text)