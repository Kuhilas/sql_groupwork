import psycopg2
from config import config

# Additional exercises. Creating a table and making multiple transactions

def create_accounts():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        # SQL statement to create the 'accounts' table with a foreign key
        SQL = """
        DROP TABLE IF EXISTS accounts;
        CREATE TABLE accounts (
            account_id SERIAL PRIMARY KEY,
            person_id INT,
            account_number VARCHAR(20),
            account_balance INT,
            FOREIGN KEY (person_id) REFERENCES person(id) ON DELETE CASCADE
        );
        """
        cursor.execute(SQL)
        con.commit()
        print("New table created successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insert_account(account_number, person_id, account_balance):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL = """
                    INSERT INTO accounts (account_number, person_id, account_balance)
                    VALUES (%s, %s, %s);
                """                

                cursor.execute(SQL, (account_number, person_id, account_balance))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def transfer_money(debit_account, credit_account, amount):
    try:
        with psycopg2.connect(**config()) as con:
            with con.cursor() as cursor:
                SQL_1 = """
                    UPDATE accounts
                    SET account_balance = account_balance - %s
                    WHERE account_number = %s AND account_balance >= %s;
                """
                cursor.execute(SQL_1, (amount, debit_account, amount))

                # Check if the debit operation was successful
                if cursor.rowcount == 0:
                    print("Insufficient funds in the debit account.")
                    con.rollback()  # Rollback the transaction if there's an issue
                    return

                # SQL query to add money to the credit account
                SQL_2 = """
                    UPDATE accounts
                    SET account_balance = account_balance + %s
                    WHERE account_number = %s;
                """
                cursor.execute(SQL_2, (amount, credit_account))

                con.commit()
                print("Transfer succesfull")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()





def create_images():
    con = None
    try:
        con = psycopg2.connect(**config())
        cursor = con.cursor()
        SQL = """
        DROP TABLE IF EXISTS images;
        CREATE TABLE images (
            image_id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            image_data BYTEA
        );
        """
        cursor.execute(SQL)
        con.commit()
        print("New table created successfully")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()

def insert_image(name, file_path):
    with psycopg2.connect(**config()) as con:
        with con.cursor() as cursor:
            with open(file_path, 'rb') as file:
                binary_data = file.read()
            SQL = "INSERT INTO images (name, image_data) VALUES (%s, %s)"
            cursor.execute(SQL, (name, binary_data))
            con.commit()

def retrieve_image(name, output_file_path):
    """
    Retrieves an image from the database and writes it to a file.
    
    Args:
        name (str): The name of the image to retrieve.
        output_file_path (str): The path to save the retrieved image.
    """
    with psycopg2.connect(**config()) as con:
        with con.cursor() as cursor:
            # SQL query to fetch the image binary data from the database by name
            SQL = "SELECT image_data FROM images WHERE name = %s"
            cursor.execute(SQL, (name,))
            result = cursor.fetchone()
            
            if result is None:
                print(f"Error: Image with name '{name}' not found.")
            else:
                # The image data is in the first column of the result
                image_data = result[0]
                
                # Write the binary data to a file
                with open(output_file_path, 'wb') as file:
                    file.write(image_data)
                print(f"Image saved to {output_file_path}")



if __name__ == '__main__':

    create_accounts()
    insert_account('400001-4679876', 2, 10000)
    transfer_money('400001-4679876', '400001-467934', 100)
    create_images()
    insert_image("doggo", "src/data/doggo.jpg")
    retrieve_image("doggo" "doggo.jpg")