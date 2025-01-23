import psycopg2
from config import config


class JoinsAndMultiTable():

    def __init__(self):
        pass


    def orders_and_customer(self, limit=None):
        """Laskee tilausten keskimääräisen arvon"""
        """Hakee tilaukset asiakkaan nimellä ja tilauksen kokonaisarvolla."""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            o.order_id,
                            c.name,
                            SUM(oi.price_at_purchase) AS total_order_value
                        FROM 
                            Orders o
                        JOIN 
                            Customers c ON o.customer_id = c.customer_id
                        JOIN 
                            Order_Items oi ON o.order_id = oi.order_id
                        GROUP BY 
                            o.order_id, c.name
                        ORDER BY 
                            total_order_value DESC;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                if results:
                    if limit and limit < len(results):
                        results = results[:limit]
                    
                    print("Orders with Customer Names and Total Values:")
                    print(f"{'Order ID':<10} {'Customer Name':<20} {'Total Order Value':<15}")
                    for row in results:
                        print(f"{row[0]:<10} {row[1]:<20} {row[2]:.2f}")
                else:
                    print("No orders found.")

        except psycopg2.Error as error:
            print(f"Database Error: {error}")

    def top_customers_total_spending(self, limit=None):
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            c.name,
                            SUM(oi.price_at_purchase) AS total_order_value
                        FROM 
                            Orders o
                        JOIN 
                            Customers c ON o.customer_id = c.customer_id
                        JOIN 
                            Order_Items oi ON o.order_id = oi.order_id
                        GROUP BY 
                            c.name
                        ORDER BY 
                            total_order_value DESC;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                if results:
                    if limit and limit < len(results):
                        results = results[:limit]
                    
                    print("\nTop customers by total spending:")
                    print(f"{'Customer Name':<20} {'Total Spending':<15}")
                    for row in results:
                        #print(row)
                        print(f"{row[0]:<20} {row[1]:<20}")
                else:
                    print("No orders found.")

        except psycopg2.Error as error:
            print(f"Database Error: {error}")


    def get_supplier_product_count(self):
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT S.supplier_id, S.name, COUNT(p.product_id) AS total_products
                        FROM SUPPLIERS S
                        JOIN PRODUCTS P ON S.supplier_id = P.supplier_id
                        GROUP BY S.supplier_id
                        ORDER BY total_products DESC;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    counter = 0
                    print(f"\nSuppliers and number of products:")
                    print(f"\n{'Supplier ID':<12} {'Name':<30} {'Total Products':<15}")
                    for row in results:
                        counter+=row[2]
                        print(f"{row[0]:<12} {row[1]:<30} {row[2]:<15}")
                    print(f"\nTotal number of products: {counter}")
        except psycopg2.Error as error:
            print(f"Database Error: {error}")

    def popular_weekday(self):
        """Hakee viikon suosituimman tilausten viikonpäivän."""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            TO_CHAR(order_date, 'Day') AS weekday,
                            COUNT(order_id) AS total_orders
                        FROM 
                            Orders
                        GROUP BY 
                            TO_CHAR(order_date, 'Day')
                        ORDER BY 
                            total_orders DESC;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()

                    print(f"\nPopular Weekdays for Orders:")
                    print(f"{'Weekday':<10} {'Total Orders':<15}")
                    print("-" * 25)
                    for row in results:
                        print(f"{row[0].strip():<10} {row[1]:<15}")
                    print()
        except psycopg2.Error as error:
            print(f"Database Error: {error}")

    def daily_orders_trend(self, limit=None):
        """Hakee päivittäisten tilausten määrän ja lajittelee ne päivämäärän mukaan."""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            order_date, 
                            COUNT(order_id) AS daily_orders
                        FROM 
                            Orders
                        GROUP BY 
                            order_date
                        ORDER BY 
                            daily_orders DESC;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()

                    print("Most popular order days:")
                    print(f"{'Date':<12} {'Daily Orders':<15}")
                    print("-" * 27)

                    if results:
                        if limit and limit < len(results):
                            results = results[:limit]

                    for row in results:
                       # print(row)
                        date_str = row[0].strftime("%Y-%m-%d")
                        print(f"{date_str:<12} {row[1]:<15}")
                    print()
        except psycopg2.Error as error:
            print(f"Database Error: {error}")

    def average_delivery_time_by_month(self):
        """Laskee keskimääräisen toimitusajan kuukausittain."""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            EXTRACT(MONTH FROM shipped_date) AS month, 
                            AVG(delivery_date - shipped_date) AS avg_delivery_time
                        FROM 
                            Shipments
                        GROUP BY 
                            month
                        ORDER BY 
                            month;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    # Tulosta tulokset
                    if results:
                        print(f"{'Month':<10} {'Avg Delivery Time (days)':<25}")
                        print("-" * 35)
                        for row in results:
                            print(f"{int(row[0]):<10} {row[1]:<25.2f}")
                    else:
                        print("No data available.")
                    print()

        except psycopg2.Error as error:
            print(f"Database Error: {error}")
