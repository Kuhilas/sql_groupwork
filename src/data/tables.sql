CREATE TABLE Suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact_info VARCHAR(255),
    country VARCHAR(50)
); -- Done

CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    supplier_id INT, -- Foreign key from Suppliers
    stock_quantity INT,
    FOREIGN KEY (supplier_id) REFERENCES Suppliers(supplier_id) ON DELETE SET NULL
); -- Done

CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    email VARCHAR(255) UNIQUE
); -- Done

CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT, -- foreign key relation to Customer table
    order_date DATE,
    order_status VARCHAR(50),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
); -- Done


CREATE TABLE Order_Items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL, -- Foreign key from Products
    quantity INT NOT NULL,
    price_at_purchase DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
);


CREATE TABLE Shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL, -- Foreign key from Orders
    shipped_date DATE,
    delivery_date DATE,
    shipping_cost DECIMAL (10, 2),
    FOREIGN KEY (order_id) REFERENCES Orders(Order_id) ON DELETE CASCADE
); -- Done 