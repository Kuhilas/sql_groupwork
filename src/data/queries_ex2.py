import psycopg2
from config import config

class GroupingAggregations():

    def __init__(self):
        pass

    def total_sales_per_category(self):
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            p.category,
                            SUM(oi.price_at_purchase) AS total_sales
                        FROM 
                            Order_Items oi
                        JOIN 
                            Products p ON oi.product_id = p.product_id
                        GROUP BY 
                            p.category
                        ORDER BY 
                            total_sales DESC;
                    """
                    cursor.execute(query)              
                    result = cursor.fetchall()
                    if result:
                        print()
                        for row in result:
                            print(f"Category: {row[0]}, Total Sales: {row[1]:.2f}")
                    else:
                        print("No sales data available.")

        except psycopg2.Error as error:
            print(f"Database Error: {error}")
    
    def average_order_value(self):
        """Laskee tilausten keskimääräisen arvon"""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            AVG(total_order_value) AS average_order_value
                        FROM (
                            SELECT 
                                o.order_id,
                                SUM(oi.price_at_purchase) AS total_order_value
                            FROM 
                                Orders o
                            JOIN 
                                Order_Items oi ON o.order_id = oi.order_id
                            GROUP BY 
                                o.order_id
                        ) subquery;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    if result and result[0] is not None:
                        print(f"\nAverage Order Value: {result[0]:.2f}")
                    else:
                        print("No data available to calculate Average Order Value.")
                        
        except psycopg2.Error as error:
            print(f"Database Error: {error}")

    def monthly_breakdown(self):
        """Kuukausittainen erittely tilausten määrästä ja kokonaismyynnistä."""
        try:
            with psycopg2.connect(**config()) as con:
                with con.cursor() as cursor:
                    query = """
                        SELECT 
                            DATE_TRUNC('month', o.order_date) AS month,
                            COUNT(DISTINCT o.order_id) AS total_orders,
                            SUM(oi.price_at_purchase) AS total_sales
                        FROM 
                            Orders o
                        JOIN 
                            Order_Items oi ON o.order_id = oi.order_id
                        GROUP BY 
                            DATE_TRUNC('month', o.order_date)
                        ORDER BY 
                            month;
                    """
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    if results:
                        print("\nMonthly Breakdown of Orders and Sales:")
                        print(f"{'Month':<15} {'Total Orders':<15} {'Total Sales':<15}")
                        for row in results:
                            print(f"{row[0].strftime('%Y-%m'):<15} {row[1]:<15} {row[2]:.2f}")
                    else:
                        print("No data available.")

        except psycopg2.Error as error:
            print(f"Database Error: {error}")