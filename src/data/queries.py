import psycopg2
from config import config
from queries_ex2 import GroupingAggregations
from queries_ex3 import JoinsAndMultiTable

# Basic Counts and Sums 
def query_counts_sums():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # Total number of orders
        SQL1 = 'SELECT COUNT(*) FROM orders;'
        cursor.execute(SQL1)
        orders = cursor.fetchone()
        print(f"\nTotal number of orders was {orders[0]}")
        # Total sales
        SQL2 = '''SELECT SUM(price_at_purchase)
                FROM Order_items;
        '''
        cursor.execute(SQL2)
        total_sales = cursor.fetchone()[0]
        print(f"\nTotal sales in 2024: {total_sales}€\n")

        SQL3 = 'SELECT product_id, name, stock_quantity FROM products WHERE stock_quantity < 10;'
        # Count of products in low stock
        cursor.execute(SQL3)
        low_stock_products = cursor.fetchall()

        print("\nProducts with low stock (stock_quantity < 10):")
        for product in low_stock_products:
            product_id, name, stock_quantity = product
            print(f"Product ID: {product_id}, Name: {name}, Stock Quantity: {stock_quantity}")
        

        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def nested_queries():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # Products that have never been ordered
        SQL1 = '''
        SELECT p.product_id, p.name, p.category, p.price, p.stock_quantity
        FROM Products p
        LEFT JOIN Order_Items oi ON p.product_id = oi.product_id
        WHERE oi.product_id IS NULL;
        '''
        cursor.execute(SQL1)
        unordered_products = cursor.fetchall()
        if unordered_products:
            print("\nProducts that have never been ordered:")
            for product in unordered_products:
                product_id, name, category, price, stock_quantity = product
                print(f"Product ID: {product_id}, Name: {name}, Category: {category}, Price: {price}€, Stock: {stock_quantity}")
        else:
            print("\nNo products found that have never been ordered.")
    
        # Customers who placed orders above desired value
        desired_value = 10000
        SQL2 = '''
        SELECT customer_id, name, email
        FROM Customers
        WHERE customer_id IN (
            SELECT customer_id
            FROM Orders
            WHERE (
                SELECT SUM(price_at_purchase)
                FROM Order_Items
                WHERE Order_Items.order_id = Orders.order_id
            ) > %s
        );
        '''
        cursor.execute(SQL2, (desired_value,))
        high_value_customers = cursor.fetchall()

        if high_value_customers:
            print(f"\nCustomers with total order value above {desired_value}€:")
            for customer in high_value_customers:
                customer_id, name, email = customer
                print(f"Customer ID: {customer_id}, Name: {name}, Email: {email}")
        else:
            print(f"\nNo customers found with total order value above {desired_value}€.")

        # Top 10 orders with the highest number of items
        SQL3 = '''
        SELECT o.order_id, c.name AS customer_name, SUM(oi.quantity) AS total_items, SUM(oi.price_at_purchase) AS total_order_value
        FROM Orders o
        JOIN Customers c ON o.customer_id = c.customer_id
        JOIN Order_Items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, c.name
        ORDER BY total_items DESC
        LIMIT 10;
        '''
        cursor.execute(SQL3)
        top_orders = cursor.fetchall()

        if top_orders:
            print("\nTop 10 Orders with the highest number of items:")
            for order in top_orders:
                order_id, customer_name, total_items, total_order_value = order
                print(f"Order ID: {order_id}, Customer: {customer_name}, Total Items: {total_items}, Total Order Value: {total_order_value}€")
        else:
            print("\nNo orders found.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if con is not None:
            con.close()

def calculate_top_10():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()

        # Retrieve total sales in 2024
        SQL_total_sales = '''SELECT SUM(price_at_purchase) FROM Order_Items;'''
        cursor.execute(SQL_total_sales)
        total_sales_2024 = cursor.fetchone()[0]

        # Query the top 10 products by total sales
        SQL_top_10 = '''
        SELECT p.product_id, p.name, SUM(oi.price_at_purchase) AS total_sales
        FROM Products p
        JOIN Order_Items oi ON p.product_id = oi.product_id
        GROUP BY p.product_id, p.name
        ORDER BY total_sales DESC;
        '''
        cursor.execute(SQL_top_10)
        product_sales = cursor.fetchall()

        # Calculate the number of products that constitute the top 10% by sales
        total_products = len(product_sales)
        top_10_percent_count = max(1, total_products // 10)

        # Calculate the total sales of the top 10% of products
        top_10_percent_sales = sum(sales[2] for sales in product_sales[:top_10_percent_count])

        # Calculate the percentage of total sales contributed by the top 10% of products
        sales_percentage = (top_10_percent_sales / total_sales_2024) * 100

        print(f"\nTop 10% of products contributed {top_10_percent_sales:.2f}€ in sales, which is {sales_percentage:.2f}% of total sales in 2024.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if con is not None:
            con.close()




if __name__ == '__main__':
    query_counts_sums()
    # aggregations = GroupingAggregations()
    aggregations.total_sales_per_category()
    aggregations.average_order_value()
    # aggregations.monthly_breakdown()
    nested_queries()
    calculate_top_10()
    joins_multitable = JoinsAndMultiTable()
    # joins_multitable.orders_and_customer(10)
    joins_multitable.top_customers_total_spending(10)
    joins_multitable.popular_weekday()
    joins_multitable.daily_orders_trend(5)
    joins_multitable.average_delivery_time_by_month()