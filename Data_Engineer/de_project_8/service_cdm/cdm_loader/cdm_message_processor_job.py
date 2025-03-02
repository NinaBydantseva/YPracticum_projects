from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:

        self._logger = logger        
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size

    def run(self) -> None:

        self._logger.info(f"{datetime.utcnow()}: START")
    #Организуйте цикл из _batch_size итераций        
        for batch_number in range(self._batch_size):
    #Получите сообщение из Kafka с помощью consume
            dds_msg = self._consumer.consume()
            self._logger.info(f"{datetime.utcnow()}: {dds_msg}")            
    #Учтите, что если все сообщения из Kafka обработаны, то consume вернёт None. В этом случае стоит прекратить обработку раньше        
            if not dds_msg:
                break
            self._logger.info(f"{datetime.utcnow()}: Message received")
    #Разбираем входящее сообщение в цикле:
            for it in dds_msg:
                h_order_pk=it["h_order_pk"]
                user_id=it["h_user_pk"]
                product_id=it["h_product_pk"]
                product_name=it["productname"]
                category_id= it["h_category_pk"]
                category_name= it["category_name"]
                status= it["status"]
                if status=='CLOSED':
                    self._logger.info(f"{datetime.utcnow()}: {user_id}, {product_id}, {category_id}")
                    self._cdm_repository.cdm_product_insert(user_id,product_id,product_name,self._logger)
                    self._cdm_repository.cdm_category_insert(user_id,category_id,category_name, self._logger)

        self._logger.info(f"{datetime.utcnow()}: FINISH")


