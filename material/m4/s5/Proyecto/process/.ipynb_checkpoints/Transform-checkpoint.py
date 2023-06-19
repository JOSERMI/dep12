
from pandasql import sqldf
pysqldf = lambda q: sqldf(q, locals()) 

class Transform():
    
    def __init__(self) -> None:
        self.process = 'Transform Process'


    def enunciado1(self, customer, orders, items ):
        q = """
                SELECT
                    customer_id, customer_fname, customer_lname, customer_email, sum(order_item_quantity) as quantity_item_total, sum(order_item_subtotal)as total
                FROM
                    customer as c
                INNER JOIN
                    orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    items as oi
                    ON o.order_id = oi.order_item_order_id
                WHERE order_status <> 'CANCELED'
                GROUP BY customer_id, customer_fname, customer_lname, customer_email
                ORDER BY  total DESC
                LIMIT 20
            """
        result = sqldf(q)

        return result
        

    def enunciado2(self, df_order_items,df_products,df_categories):
        q = """
                SELECT
                    ca.category_name, sum(order_item_quantity) as item_quantity, cast(sum(order_item_subtotal) AS INT )as total
                FROM df_order_items as oi
                INNER JOIN
                    df_products as p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                    df_categories as ca
                    ON p.product_category_id = ca.category_id
                GROUP BY ca.category_name
            """

        result = sqldf(q)

        return result

    def enunciado3(self,df_customer,df_orders,df_order_items,df_products, df_categories):
        q = """SELECT
                customer_city, category_name
                FROM (SELECT
                    customer_city, category_name, count(category_name) as quantity, DENSE_RANK () OVER ( 
                                PARTITION BY customer_city 
                                ORDER BY count(category_name) DESC
                            ) rank
                    FROM
                        df_customer as c
                    INNER JOIN
                        df_orders as o
                        ON c.customer_id = o.order_customer_id
                    INNER JOIN
                        df_order_items as oi
                        ON o.order_id = oi.order_item_order_id
                    INNER JOIN
                        df_products as p
                        ON oi.order_item_product_id = p.product_id
                    INNER JOIN
                        df_categories as ca
                        ON p.product_category_id = ca.category_id
                    GROUP BY customer_city, category_name
                    ) t
            WHERE rank = 1
            """
        result = sqldf(q)
        
        return result


    def enunciado4(self,df_customer,df_orders,df_order_items,df_products, df_categories):
        
        q = """
            SELECT
                    customer_city, product_name, quantity, total
                    FROM (SELECT
                        customer_city, product_name,sum(order_item_quantity) as quantity,sum(order_item_subtotal) as total, DENSE_RANK () OVER ( 
                                    PARTITION BY customer_city 
                                    ORDER BY sum(order_item_quantity) DESC
                                ) rank
                        FROM
                            df_customer as c
                        INNER JOIN
                            df_orders as o
                            ON c.customer_id = o.order_customer_id
                        INNER JOIN
                            df_order_items as oi
                            ON o.order_id = oi.order_item_order_id
                        INNER JOIN
                            df_products as p
                            ON oi.order_item_product_id = p.product_id
                        INNER JOIN
                            df_categories as ca
                            ON p.product_category_id = ca.category_id
                        GROUP BY customer_city, category_name
                        ) t
                WHERE rank < 6
                ORDER BY quantity DESC
            """
        result = sqldf(q)
        
        return result


    def enunciado5():
        pass

    def customer_orders(df_customer, df_orders, df_order_items, df_departments, df_categories, df_products):
        
        customer = df_customer
        orders = df_orders
        order_items = df_order_items
        departments = df_departments
        categories = df_categories
        products = df_products


        q = """
                SELECT
                    customer_id,order_date,order_status,order_item_quantity,order_item_subtotal,order_item_product_price,product_name,product_description,product_price,product_image,category_name,department_name
                FROM
                    customer as c
                INNER JOIN
                    orders as o
                    ON c.customer_id = o.order_customer_id
                INNER JOIN
                    order_items as oi
                    ON o.order_id = oi.order_item_order_id
                INNER JOIN
                    products as p
                    ON oi.order_item_product_id = p.product_id
                INNER JOIN
                    categories as ca
                    ON p.product_category_id = ca.category_id
                INNER JOIN
                    departments as d
                    ON ca.category_department_id = d.department_id;
            """
        joined = sqldf(q)
        df_final = joined.groupby('customer_id').apply(lambda x: x.drop(['customer_id'], axis=1).to_dict(orient='records'))
    
        return df_final
    

    def enunciado20(self, df_customer, df_orders, df_order_items):
        
        result = df_customer[["customer_id","customer_fname"]]\
                    .merge(df_orders[["order_id","order_date","order_customer_id"]], left_on="customer_id", right_on="order_customer_id" , how="left")\
                    .merge(df_order_items[["order_item_id","order_item_order_id","order_item_product_id"]], left_on="order_id", right_on="order_item_order_id",how="left")

        #result = df_customer\
        #            .merge(df_orders, left_on="customer_id", right_on="order_customer_id" , how="left")\
        #            .merge(df_order_items, left_on="order_id", right_on="order_item_order_id",how="left")
        
        return result