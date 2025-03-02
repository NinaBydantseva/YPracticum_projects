import json
import uuid
from logging import Logger
from typing import List, Dict
from datetime import datetime

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository, OrderDdsBuilder


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._logger = logger        
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._batch_size = batch_size

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
    #Организуйте цикл из _batch_size итераций        
        for batch_number in range(self._batch_size):
    #Получите сообщение из Kafka с помощью consume
            msg = self._consumer.consume()
            self._logger.info(f"{datetime.utcnow()}: {msg}")            
    #Прекращаем обработку с окончанием сообщений        
            if not msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
    #Сохраняем данные из топика для разбора по таблицам
            order = msg['payload']             
    #Заполняем таблицу пользователей user и s_user_names
            self._dds_repository.h_user_insert(OrderDdsBuilder(order,self._logger).h_user())
            self._dds_repository.s_user_names_insert(OrderDdsBuilder(order,self._logger).s_user_names())
    #Заполняем таблицу ресторанов restaurant и restaurant_names
            self._dds_repository.h_restaurant_insert(OrderDdsBuilder(order,self._logger).h_restaurant()) 
            self._dds_repository.s_restaurant_names_insert(OrderDdsBuilder(order,self._logger).s_restaurant_names())   
    #Заполняем таблицу заказов order и order_status  
            self._dds_repository.h_order_insert(OrderDdsBuilder(order,self._logger).h_order())
            self._dds_repository.s_order_status_insert(OrderDdsBuilder(order,self._logger).s_order_status())
    #Заполняем таблицы продуктов и категорий в цикле:
            self._dds_repository.h_product_insert(OrderDdsBuilder(order,self._logger).h_product())
            self._dds_repository.s_product_names_insert(OrderDdsBuilder(order,self._logger).s_product_names())
            self._dds_repository.h_category_insert(OrderDdsBuilder(order,self._logger).h_category())
            self._dds_repository.s_order_cost_insert(OrderDdsBuilder(order,self._logger).s_order_cost())
    #Заполняем таблицы связей
            self._dds_repository.l_order_user_insert(OrderDdsBuilder(order,self._logger).l_order_user())    
            self._dds_repository.l_order_product_insert(OrderDdsBuilder(order,self._logger).l_order_product()) 
            self._dds_repository.l_product_restaurant_insert(OrderDdsBuilder(order,self._logger).l_product_restaurant())
            self._dds_repository.l_product_category_insert(OrderDdsBuilder(order,self._logger).l_product_category())
    #Отправляем выходное сообщение в producer
            dds_msg = self._dds_repository.dds_msg_select(OrderDdsBuilder(order, self._logger)._uuid(order['id']))
            self._logger.info(f"{datetime.utcnow()}: {dds_msg}")
            cdm_msg=self._format_items(dds_msg)
            self._logger.info(f"{datetime.utcnow()}: {cdm_msg}")            
            topic_name=self._producer.topic
            self._logger.info(f"{datetime.utcnow()}: {topic_name}")            
            self._producer.produce(cdm_msg)
            self._logger.info(f"{datetime.utcnow()}. Message Sent")     
    # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")
    #Формируем выходное сообщение.

    def _format_items(self, dds_msg) -> List[Dict[str, str]]:
          items = []

          for prd in dds_msg:
                        cdm_prd = {
                                "h_order_pk": str(prd[0]),
                                "h_user_pk": str(prd[1]),
                                "h_product_pk": str(prd[2]),
                                "productname": prd[3],
                                "h_category_pk": str(prd[4]),
                                "category_name": prd[5],
                                "status": prd[6]
                                }
                        items.append(cdm_prd)
          return items              
