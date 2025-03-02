import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from logging import Logger



class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def cdm_product_insert(self,
                       user_id: uuid,
                       product_id: uuid,
                       product_name: str,
                       logger: Logger) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"select count(*) from cdm.user_product_counters upc where upc.user_id::text='{user_id}' and upc.product_id::text='{product_id}'")
                result=cur.fetchone()
                order_cnt=result[0]
                logger.info(f"{datetime.utcnow()}: SELECT prd {order_cnt}") 
                if order_cnt==0: 
                    order_cnt=1    
                    logger.info(f"{datetime.utcnow()}: UUID('{user_id}'),UUID('{product_id}'),'{product_name}',{order_cnt}") 
                    cur.execute(
                        f"""
                            INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                            VALUES (UUID('{user_id}'),UUID('{product_id}'),'{product_name}',{order_cnt});
                        """
                   #     ,
                   #     {   'user_id': user_id,
                   #         'product_id': product_id,
                   #         'product_name': product_name,
                   #         'order_cnt': order_cnt
                   #     }
                    )
                    logger.info(f"{datetime.utcnow()}: INSERT prd {order_cnt}")   
                else:
                    cur.execute(f"UPDATE cdm.user_product_counters SET order_cnt=order_cnt+1 where user_id::text='{user_id}' and product_id::text='{product_id}'")   
                    logger.info(f"{datetime.utcnow()}: UPDATE prd {order_cnt}") 

    def cdm_category_insert(self,
                       user_id: uuid,
                       category_id: uuid,
                       category_name: str,
                       logger: Logger) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"select count(*) from cdm.user_category_counters ucc where ucc.user_id::text='{user_id}' and ucc.category_id::text='{category_id}'")
                result=cur.fetchone()
                order_cnt=result[0]
                logger.info(f"{datetime.utcnow()}: SELECT cat {order_cnt}") 
                if order_cnt==0: 
                    order_cnt=1          
                    logger.info(f"{datetime.utcnow()}: UUID('{user_id}'),UUID('{category_id}'),'{category_name}',{order_cnt}")                                            
                    cur.execute(
                        f"""
                            INSERT INTO cdm.user_category_counters (user_id,category_id,category_name,order_cnt)
                            VALUES (UUID('{user_id}'),UUID('{category_id}'),'{category_name}',{order_cnt})
                        """
                    #    ,
                    #    {   'user_id': user_id,
                    #        'category_id': category_id,
                    #        'category_name': category_name,
                    #        'order_cnt': order_cnt
                    #    }                    
                    )  
                    logger.info(f"{datetime.utcnow()}: INSERT cat {order_cnt}")  
                else:
                    cur.execute(f"UPDATE cdm.user_category_counters SET order_cnt=order_cnt+1 where user_id::text='{user_id}' and category_id::text='{category_id}'") 
                    logger.info(f"{datetime.utcnow()}: UPDATE cat {order_cnt}")                                    