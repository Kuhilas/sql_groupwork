import psycopg2
from config import config
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

def query():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor: 
                SQL = 'SELECT * FROM products;'
                cursor.execute(SQL)
                columns = [desc[0] for desc in cursor.description]
                print(columns)

    except psycopg2.Error as error: 
        print(error)


def generate_customers():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                for i in range(995):
                    name = fake.name()
                    location = fake.city()
                    email = f"{name.split()[0].lower()}.{name.split()[1].lower()}@{fake.domain_name()}"
                    SQL_insert = """ INSERT INTO Customers (name, location, email)
                                VALUES (%s, %s, %s);"""

                    cursor.execute(SQL_insert, (name, location, email))

                con.commit()
                print("Table Person populated with 1000 rows")
    
    except psycopg2.Error as error:
        print(error) 

def generate_supplier():
    countries = ['USA', 'Germany', 'China', 'Japan', 'South Korea', 'Finland', 'India']
    return {
        "name": fake.company(),
        "contact_info": fake.email(),
        "country": random.choice(countries)
    }

def generate_product(suppliers_amount: int):
    adjectives = ['Super', 'Pro', 'Ultra', 'Max', 'Elite', 'Neo', 'Mini']
    categories = ['smartphone', 'laptop', 'tablet', 'headphones', 'smartwatch', 'television', 'camera', 'gaming console']
    
    category = random.choice(categories)  
    adjective = random.choice(adjectives)  
    
    return {
        "name": f"{adjective} {category}", 
        "category": category,
        "price": round(random.uniform(100, 1000), 2),
        "supplier_id": random.randint(1, suppliers_amount),
        "stock_quantity": random.randint(1, 100)
    }

def generate_suppliers_data(amount: int):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                suppliers = [generate_supplier() for _ in range(amount)]
                insert_query = "INSERT INTO Suppliers (name, contact_info, country) VALUES (%s, %s, %s)"            
                cursor.executemany(insert_query, 
                                    [(supplier['name'], supplier['contact_info'], supplier['country']) for supplier in suppliers])               
                con.commit()

                print(f"{amount} suppliers have been inserted into the database!")
    
    except psycopg2.Error as error: 
        print(f"Error: {error}")

def generate_products_data(amount: int, suppliers_amount: int):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                products = [generate_product(suppliers_amount) for _ in range(amount)]
                insert_query = "INSERT INTO Products (name, category, price, supplier_id, stock_quantity) VALUES (%s, %s, %s, %s, %s)"               
                cursor.executemany(insert_query, 
                                    [(product['name'], product['category'], product['price'], product['supplier_id'], product['stock_quantity']) for product in products])             
                con.commit()
                print(f"{amount} products have been inserted into the database!")
    
    except psycopg2.Error as error: 
        print(f"Error: {error}")

def truncate_table(table_name):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                query = f"TRUNCATE TABLE {table_name} CASCADE;"
                cursor.execute(query)
                table_name_without_last_char = table_name[:-1]
                cursor.execute(f"ALTER SEQUENCE {table_name}_{table_name_without_last_char}_id_seq RESTART WITH 1;")
                print(f"Table '{table_name}' has been truncated.")
    
    except psycopg2.Error as error:
        print(f"Error: {error}")

def generate_order():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                # Haetaan satunnainen customer_id customers-taulusta
                cursor.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1;")
                customer_id = cursor.fetchone()[0]
                
                # Satunnainen tilauksen tila
                order_status = random.choice(['Pending', 'Shipped', 'Delivered'])

                order_date = fake.date_between(start_date=datetime(2024, 1, 1).date(), end_date=datetime(2024, 12, 31).date())

                return customer_id, order_status, order_date

    except psycopg2.Error as error:
        print(f"Error: {error}")


def generate_orders(amount: int):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                orders = [generate_order() for _ in range(amount)]
                insert_query = "INSERT INTO Orders (customer_id, order_status, order_date) VALUES (%s, %s, %s)"
                
                cursor.executemany(insert_query, orders)
                
                con.commit()

                print(f"{amount} orders have been inserted into the database!")

    except psycopg2.Error as error: 
        print(f"Error: {error}")

def generate_shipments():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                cursor.execute("SELECT order_id, order_date, order_status FROM Orders WHERE order_status IN ('Shipped', 'Delivered');")
                orders = cursor.fetchall()

                shipments = []
                for order in orders:
                    order_id, order_date, order_status = order
                    
                    shipped_date = order_date
                    
                    delivery_days = random.randint(2, 7)
                    delivery_date = shipped_date + timedelta(days=delivery_days)
                    
                    shipping_cost = round(random.uniform(5, 50), 2)
                    
                    shipments.append((order_id, shipped_date, delivery_date, shipping_cost))

                insert_query = """
                    INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_cost)
                    VALUES (%s, %s, %s, %s);
                """
                cursor.executemany(insert_query, shipments)

                con.commit()

                print(f"{len(shipments)} shipments have been inserted into the database!")
    
    except psycopg2.Error as error:
        print(f"Error: {error}")

def generate_order_item(order_id):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                # Haetaan satunnainen product_id Products-taulusta ja tuotteen hinta
                cursor.execute("SELECT product_id, price FROM Products ORDER BY RANDOM() LIMIT 1;")
                product_result = cursor.fetchone()
                if product_result:
                    product_id, price = product_result
                else:
                    raise ValueError("No products found in the 'Products' table")
                
                # Satunnainen määrä (quantity)
                quantity = random.randint(1, 10)  
                price_at_purchase = price * quantity

                return order_id, product_id, quantity, price_at_purchase

    except psycopg2.Error as error:
        print(f"Database Error: {error}")
    except ValueError as e:
        print(f"Error: {e}")

# Funktio, joka lisää Order Itemeja jokaiselle tilaukselle
def generate_order_items():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                # Haetaan kaikki tilaukset Orders-taulusta
                cursor.execute("SELECT order_id FROM Orders;")
                order_ids = cursor.fetchall()

                if not order_ids:
                    print("No orders found in the 'Orders' table.")
                    return
                
                # Luodaan Order Items jokaiselle tilaukselle
                order_items = []
                for order_id_tuple in order_ids:
                    order_id = order_id_tuple[0]
                    order_item = generate_order_item(order_id)
                    if order_item:
                        order_items.append(order_item)

                # Suoritetaan vain, jos order_items lista ei ole tyhjä
                if order_items:
                    insert_query = """
                        INSERT INTO Order_Items (order_id, product_id, quantity, price_at_purchase) 
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, order_items)
                    con.commit()
                    print(f"{len(order_items)} order items have been inserted into the database!")
                else:
                    print("No order items to insert.")

    except psycopg2.Error as error:
        print(f"Database Error: {error}")

def generate_random_order_item():
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                cursor.execute("SELECT order_id FROM Orders ORDER BY RANDOM() LIMIT 1;")
                result = cursor.fetchone()
                if result:
                    order_id = result[0]
                else:
                    raise ValueError("No orders found in the 'Orders' table")
                
                cursor.execute("SELECT product_id, price FROM Products ORDER BY RANDOM() LIMIT 1;")
                product_result = cursor.fetchone()
                if product_result:
                    product_id, price = product_result
                else:
                    raise ValueError("No products found in the 'Products' table")
                
                quantity = random.randint(1, 10) 
                price_at_purchase = price*quantity

                return order_id, product_id, quantity, price_at_purchase

    except psycopg2.Error as error:
        print(f"Database Error: {error}")
    except ValueError as e:
        print(f"Error: {e}")

def generate_random_order_items(amount: int):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                order_items = [generate_random_order_item() for _ in range(amount)]
                
                # Suoritetaan vain, jos order_items lista ei ole tyhjä
                if order_items:
                    insert_query = """
                        INSERT INTO Order_Items (order_id, product_id, quantity, price_at_purchase) 
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.executemany(insert_query, order_items)
                    con.commit()
                    print(f"{amount} order items have been inserted into the database!")
                else:
                    print("No order items to insert.")

    except psycopg2.Error as error:
        print(f"Database Error: {error}")

if __name__ == '__main__':
    # generate_customers()
    # generate_suppliers_data(30)
    # generate_products_data(12, 30)
    # truncate_table('suppliers')
    # generate_orders(1000)
    # generate_shipments() # Shipments are created based on shipped/delivered orders, no user input
    # generate_order_items()
    # generate_random_order_items(200)