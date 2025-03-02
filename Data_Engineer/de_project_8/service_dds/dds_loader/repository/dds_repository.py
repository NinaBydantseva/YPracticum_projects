import uuid
from datetime import datetime
from typing import Any, Dict, List
from logging import Logger

from lib.pg import PgConnect
from pydantic import BaseModel


class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel):
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str

class H_Restaurant (BaseModel):
	h_restaurant_pk: uuid.UUID
	restaurant_id: str
	load_dt: datetime
	load_src: str

class H_Order(BaseModel):
	h_order_pk: uuid.UUID
	order_id: str
	order_dt: datetime
	load_dt: datetime
	load_src: str

class H_Category(BaseModel):
	h_category_pk: uuid.UUID
	category_name: str
	load_dt: datetime
	load_src: str

class S_User_names (BaseModel):
	h_user_pk: uuid.UUID
	username: str
	userlogin: str
	load_dt: datetime
	load_src: str
	hk_user_names_hashdiff: uuid.UUID

class S_Restaurant_names (BaseModel):
	h_restaurant_pk: uuid.UUID
	restaurantname: str
	load_dt: datetime
	load_src: str
	hk_restaurant_names_hashdiff: uuid.UUID

class S_Product_names (BaseModel):
	h_product_pk: uuid.UUID
	productname: str
	load_dt: datetime
	load_src: str
	hk_product_names_hashdiff: uuid.UUID

class S_Order_status (BaseModel):
	h_order_pk: uuid.UUID
	status: str
	load_dt: datetime
	load_src: str
	hk_order_status_hashdiff: uuid.UUID

class S_Order_cost (BaseModel):
	h_order_pk: uuid.UUID
	ordercost: float
	payment: float
	load_dt: datetime
	load_src: str
	hk_order_cost_hashdiff: uuid.UUID

class L_Order_product(BaseModel):
	hk_order_product_pk: uuid.UUID
	h_order_pk: uuid.UUID
	h_product_pk: uuid.UUID
	load_dt: datetime
	load_src: str

class L_Order_user (BaseModel):
	hk_order_user_pk: uuid.UUID
	h_order_pk: uuid.UUID
	h_user_pk: uuid.UUID
	load_dt: datetime
	load_src: str

class L_Product_category (BaseModel):
	hk_product_category_pk: uuid.UUID
	h_product_pk: uuid.UUID
	h_category_pk: uuid.UUID
	load_dt: datetime
	load_src: str

class L_Product_restaurant (BaseModel):
	hk_product_restaurant_pk: uuid.UUID
	h_product_pk: uuid.UUID
	h_restaurant_pk: uuid.UUID
	load_dt: datetime
	load_src: str

class OrderDdsBuilder:
    def __init__(self, dict: Dict, logger: Logger) -> None:
        self._dict = dict
        self.source_system = "kafka_stg_msg"
        self._logger=logger
    def _uuid(self, obj: any) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_DNS, name=str(obj))

    def h_user(self) -> H_User:
        user_id = self._dict['user']['id']
        return H_User(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_product(self) -> List[H_Product]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                 H_Product(
                    h_product_pk=self._uuid(prod_id),
                    product_id=str(prod_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system
                    )
            )

        self._logger.info(f"{datetime.utcnow()}: {products}") 
        return products 
    
    def h_restaurant(self) -> H_Restaurant:
        restaurant_id = self._dict['restaurant']['id']

        return H_Restaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=str(restaurant_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )

    def h_order(self) -> H_Order:
        order_id = self._dict['id']
        order_dt = self._dict['date']

        return H_Order (
            h_order_pk=self._uuid(order_id),
            order_id=str(order_id),
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system
        )
    
    def h_category(self) -> List[H_Category]:
        categories = []

        for cat_dict in self._dict['products']:
            cat_name = cat_dict['category']
            categories.append(
                H_Category(
                    h_category_pk=self._uuid(cat_name),
                    category_name=cat_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system  
                )
            )
        return categories

    def s_user_names(self) -> S_User_names:
        user_id = self._dict['user']['id']
        username = self._dict['user']['name']
        userlogin = self._dict['user']['login']

        return S_User_names(
            h_user_pk=self._uuid(user_id),
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(f"{user_id}#$#{username}#$#{userlogin}")           
        )
    def s_restaurant_names(self) -> S_Restaurant_names:
        restaurant_id = self._dict['restaurant']['id']
        restaurantname = self._dict['restaurant']['name']

        return S_Restaurant_names(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurantname=restaurantname,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(f"{restaurant_id}#$#{restaurantname}")               
        )   
    def s_product_names(self) -> List[S_Product_names]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            prod_name = prod_dict['name']
            products.append(
                S_Product_names(
                    h_product_pk=self._uuid(prod_id),
                    productname=prod_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(f"{prod_id}#$#{prod_name}")
                    )
                )
        return products

    def s_order_status(self) -> S_Order_status:
        order_id=self._dict['id']
        status=self._dict['status']    

        return S_Order_status(
            h_order_pk=self._uuid(order_id),
            status=status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(f"{order_id}#$#{status}")         
        )
    def s_order_cost(self) -> S_Order_cost:
        order_id=self._dict['id']
        ordercost=self._dict['cost'] 
        payment=self._dict['payment']

        return S_Order_cost(
            h_order_pk=self._uuid(order_id),
            ordercost=ordercost,
            payment=payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(f"{order_id}#$#{ordercost}#$#{payment}")             
        )

    def l_order_product(self) -> List[L_Order_product]:
        order_id=self._dict['id']        
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            products.append(
                L_Order_product(
                hk_order_product_pk=self._uuid(f"{order_id}#$#{prod_id}"),
                h_order_pk=self._uuid(order_id),
                h_product_pk=self._uuid(prod_id),
                load_dt=datetime.utcnow(),
                load_src=self.source_system 
                )
            )
        return products

    def l_order_user(self) -> L_Order_user:
        order_id=self._dict['id']
        user_id=self._dict['user']['id']

        return L_Order_user(
            hk_order_user_pk=self._uuid(f"{order_id}#$#{user_id}"),
            h_order_pk=self._uuid(order_id),
            h_user_pk=self._uuid(user_id),
            load_dt=datetime.utcnow(),
            load_src=self.source_system            
        )   
    def l_product_category(self) -> List[L_Product_category]:
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            cat_name = prod_dict['category']
            products.append(
                L_Product_category(
                    hk_product_category_pk=self._uuid(f"{prod_id}#$#{cat_name}"),
                    h_product_pk=self._uuid(prod_id),
                    h_category_pk=self._uuid(cat_name),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system 
                )
            )
        return products

    def l_product_restaurant(self) -> List[L_Product_restaurant]:
        restaurant_id = self._dict['restaurant']['id']
        products = []

        for prod_dict in self._dict['products']:
            prod_id = prod_dict['id']
            restaurant_id = self._dict['restaurant']['id']
            products.append(
                L_Product_restaurant(
                    hk_product_restaurant_pk=self._uuid(f"{prod_id}#$#{restaurant_id}"), 
                    h_product_pk=self._uuid(prod_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system 
                )
            )
        return products

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )

    def h_product_insert(self, obj: List[H_Product]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.h_product(
                                h_product_pk,
                                product_id,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(product_id)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_product_pk) DO NOTHING;
                        """,
                        {
                            'h_product_pk': x.h_product_pk,
                            'product_id': x.product_id,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src
                        }
                    )

    def h_restaurant_insert(self, restaurant: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )

    def h_order_insert(self, order: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )

    def h_category_insert(self, obj: List[H_Category]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.h_category(
                                h_category_pk,
                                category_name,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(h_category_pk)s,
                                %(category_name)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (h_category_pk) DO NOTHING;
                        """,
                        {
                            'h_category_pk': x.h_category_pk,
                            'category_name': x.category_name,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src
                        }
                    )

    def s_user_names_insert(self, obj: S_User_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk,
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (hk_user_names_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_user_pk': obj.h_user_pk,
                        'username': obj.username,
                        'userlogin': obj.userlogin,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_user_names_hashdiff': obj.hk_user_names_hashdiff
                    }
                )

    def s_restaurant_names_insert(self, obj: S_Restaurant_names) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk,
                            restaurantname,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurantname)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (hk_restaurant_names_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': obj.h_restaurant_pk,
                        'restaurantname': obj.restaurantname,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_restaurant_names_hashdiff': obj.hk_restaurant_names_hashdiff
                    }
                )

    def s_product_names_insert(self, obj: List[S_Product_names]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.s_product_names(
                                h_product_pk,
                                productname,
                                load_dt,
                                load_src,
                                hk_product_names_hashdiff
                            )
                            VALUES(
                                %(h_product_pk)s,
                                %(productname)s,
                                %(load_dt)s,
                                %(load_src)s,
                                %(hk_product_names_hashdiff)s
                            )
                            ON CONFLICT (hk_product_names_hashdiff) DO NOTHING;
                        """,
                        {
                            'h_product_pk': x.h_product_pk,
                            'productname': x.productname,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src,
                            'hk_product_names_hashdiff': x.hk_product_names_hashdiff
                        }
                    )

    def s_order_status_insert(self, obj: S_Order_status) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk,
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (hk_order_status_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'status': obj.status,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_status_hashdiff': obj.hk_order_status_hashdiff
                    }
                )

    def s_order_cost_insert(self, obj: S_Order_cost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk,
                            ordercost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(ordercost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (hk_order_cost_hashdiff) DO NOTHING;
                    """,
                    {
                        'h_order_pk': obj.h_order_pk,
                        'ordercost': obj.ordercost,
                        'payment': obj.payment,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src,
                        'hk_order_cost_hashdiff': obj.hk_order_cost_hashdiff
                    }
                )

    def l_order_product_insert(self, obj: List[L_Order_product]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_order_product(
                                hk_order_product_pk,
                                h_order_pk,
                                h_product_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_order_product_pk)s,
                                %(h_order_pk)s,
                                %(h_product_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_order_product_pk) DO NOTHING;
                        """,
                        {
                            'hk_order_product_pk': x.hk_order_product_pk,
                            'h_order_pk': x.h_order_pk,
                            'h_product_pk': x.h_product_pk,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src
                        }
                    )

    def l_order_user_insert(self, obj: L_Order_user) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                 cur.execute(
                    """
                        INSERT INTO dds.l_order_user(
                            hk_order_user_pk,
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': obj.hk_order_user_pk,
                        'h_order_pk': obj.h_order_pk,
                        'h_user_pk': obj.h_user_pk,
                        'load_dt': obj.load_dt,
                        'load_src': obj.load_src
                    }
                )

    def l_product_category_insert(self, obj: List[L_Product_category]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_category(
                                hk_product_category_pk,
                                h_product_pk,
                                h_category_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_category_pk)s,
                                %(h_product_pk)s,
                                %(h_category_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_category_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_category_pk': x.hk_product_category_pk,
                            'h_product_pk': x.h_product_pk,
                            'h_category_pk': x.h_category_pk,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src
                        }
                    )

    def l_product_restaurant_insert(self, obj: List[L_Product_restaurant]) -> None:
        for x in obj:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                            INSERT INTO dds.l_product_restaurant(
                                hk_product_restaurant_pk,
                                h_product_pk,
                                h_restaurant_pk,
                                load_dt,
                                load_src
                            )
                            VALUES(
                                %(hk_product_restaurant_pk)s,
                                %(h_product_pk)s,
                                %(h_restaurant_pk)s,
                                %(load_dt)s,
                                %(load_src)s
                            )
                            ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                        """,
                        {
                            'hk_product_restaurant_pk': x.hk_product_restaurant_pk,
                            'h_product_pk': x.h_product_pk,
                            'h_restaurant_pk': x.h_restaurant_pk,
                            'load_dt': x.load_dt,
                            'load_src': x.load_src
                        }
                    )
 
    def dds_msg_select(self,
                       h_order_pk: uuid): 
        with self._db.connection() as conn:
            with conn.cursor() as cur:               
                 cur.execute(f"""SELECT lou.h_order_pk, hu.h_user_pk , hp.h_product_pk, spn.productname, hc.h_category_pk, hc.category_name , sos.status
                        FROM dds.l_order_user lou
                        join dds.h_user hu on lou.h_user_pk =hu.h_user_pk
                        join dds.s_order_status sos on lou.h_order_pk=sos.h_order_pk
                        join dds.l_order_product lop on lou.h_order_pk=lop.h_order_pk
                        join dds.s_product_names spn on lop.h_product_pk=spn.h_product_pk
                        join dds.l_product_category lpc on lop.h_product_pk=lpc.h_product_pk
                        join dds.h_category hc on lpc.h_category_pk=hc.h_category_pk
                        join dds.h_product hp on lop.h_product_pk=hp.h_product_pk    
                        where lou.h_order_pk='{h_order_pk}';""")
                
                 return cur.fetchall()