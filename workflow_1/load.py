# Importing necessary libraries
import pyodbc
import csv

# Database connection parameters
server = "DESKTOP-J1LJSLQ\SQLEXPRESS"
database = "test"
username = "taha"
password = "tahataha"

# Establishing a connection to the SQL Server
cnxn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};\
                      SERVER="
    + server
    + ";\
                      DATABASE="
    + database
    + ";\
                      UID="
    + username
    + ";\
                      PWD="
    + password
)

cursor = cnxn.cursor()

# ////
# Specify the CSV file path
fact_table_path = r"C:\Users\havet\Desktop\workflow_1\data\fact_table.csv"

# Read the CSV file and insert each row into the SQL Server table
with open(fact_table_path, "r") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)

    # Skip the header row
    header = next(csv_reader)

    # Assuming your table has the same column names as the CSV header
    columns_definition = ", ".join([f"{column} INT NOT NULL" for column in header])

    # Check if fact_table exists and delete it if it does
    if cursor.tables(table="fact_table").fetchone():
        cursor.execute("DROP TABLE fact_table")
        cnxn.commit()
        print("Existing fact_table dropped.")

    # Create fact_table
    staging_table_query = f"""
        CREATE TABLE fact_table (
            {columns_definition}
        )
    """
    cursor.execute(staging_table_query)
    cnxn.commit()
    print("fact_table created.")

    # Insert data into fact_table
    insert_query = f"INSERT INTO fact_table ({', '.join(header)}) VALUES ({', '.join(['?'] * len(header))})"
    for row in csv_reader:
        cursor.execute(insert_query, row)
        cnxn.commit()

    print("Data from CSV file successfully inserted into fact_table.")
# ////
# ////
# Specify the CSV file path
product_dimension_path = r"C:\Users\havet\Desktop\workflow_1\data\product_dimension.csv"

# Read the CSV file and insert each row into the SQL Server table
with open(product_dimension_path, "r") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)

    # Skip the header row
    header = next(csv_reader)

    # Assuming your table has the same column names as the CSV header
    columns_definition = ", ".join([f"{column} VARCHAR(255)" for column in header])

    # Check if product_dimension exists and delete it if it does
    if cursor.tables(table="product_dimension").fetchone():
        cursor.execute("DROP TABLE product_dimension")
        cnxn.commit()
        print("Existing product_dimension dropped.")

    # Create product_dimension
    staging_table_query = f"""
        CREATE TABLE product_dimension (
            {columns_definition}
        )
    """
    cursor.execute(staging_table_query)
    cnxn.commit()
    print("product_dimension created.")

    # Insert data into product_dimension
    insert_query = f"INSERT INTO product_dimension ({', '.join(header)}) VALUES ({', '.join(['?'] * len(header))})"
    for row in csv_reader:
        cursor.execute(insert_query, row)
        cnxn.commit()

    print("Data from CSV file successfully inserted into product_dimension.")
# ////
# ////
# Specify the CSV file path
user_dimension_path = r"C:\Users\havet\Desktop\workflow_1\data\user_dimension.csv"

# Read the CSV file and insert each row into the SQL Server table
with open(user_dimension_path, "r") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)

    # Skip the header row
    header = next(csv_reader)

    # Assuming your table has the same column names as the CSV header
    columns_definition = ", ".join([f"{column} VARCHAR(255)" for column in header])

    # Check if user_dimension exists and delete it if it does
    if cursor.tables(table="user_dimension").fetchone():
        cursor.execute("DROP TABLE user_dimension")
        cnxn.commit()
        print("Existing user_dimension dropped.")

    # Create user_dimension
    staging_table_query = f"""
        CREATE TABLE user_dimension (
            {columns_definition}
        )
    """
    cursor.execute(staging_table_query)
    cnxn.commit()
    print("user_dimension created.")

    # Insert data into user_dimension
    insert_query = f"INSERT INTO user_dimension ({', '.join(header)}) VALUES ({', '.join(['?'] * len(header))})"
    for row in csv_reader:
        cursor.execute(insert_query, row)
        cnxn.commit()

    print("Data from CSV file successfully inserted into user_dimension.")
# ////
# ////
# Specify the CSV file path
date_dimension_path = r"C:\Users\havet\Desktop\workflow_1\data\date_dimension.csv"

# Read the CSV file and insert each row into the SQL Server table
with open(date_dimension_path, "r") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)

    # Skip the header row
    header = next(csv_reader)

    # Assuming your table has the same column names as the CSV header
    columns_definition = ", ".join([f"{column} VARCHAR(255)" for column in header])

    # Check if date_dimension exists and delete it if it does
    if cursor.tables(table="date_dimension").fetchone():
        cursor.execute("DROP TABLE date_dimension")
        cnxn.commit()
        print("Existing date_dimension dropped.")

    # Create date_dimension
    staging_table_query = f"""
        CREATE TABLE date_dimension (
            {columns_definition}
        )
    """
    cursor.execute(staging_table_query)
    cnxn.commit()
    print("date_dimension created.")

    # Insert data into date_dimension
    insert_query = f"INSERT INTO date_dimension ({', '.join(header)}) VALUES ({', '.join(['?'] * len(header))})"
    for row in csv_reader:
        cursor.execute(insert_query, row)
        cnxn.commit()

    print("Data from CSV file successfully inserted into date_dimension.")
# ////
# ////
# Specify the CSV file path
comment_dimension_path = r"C:\Users\havet\Desktop\workflow_1\data\comment_dimension.csv"

# Read the CSV file and insert each row into the SQL Server table
with open(comment_dimension_path, "r") as file:
    # Create a CSV reader object
    csv_reader = csv.reader(file)

    # Skip the header row
    header = next(csv_reader)

    # Assuming your table has the same column names as the CSV header
    columns_definition = ", ".join([f"{column} VARCHAR(255)" for column in header])

    # Check if comment_dimension exists and delete it if it does
    if cursor.tables(table="comment_dimension").fetchone():
        cursor.execute("DROP TABLE comment_dimension")
        cnxn.commit()
        print("Existing comment_dimension dropped.")

    # Create comment_dimension
    staging_table_query = f"""
        CREATE TABLE comment_dimension (
            {columns_definition}
        )
    """
    cursor.execute(staging_table_query)
    cnxn.commit()
    print("comment_dimension created.")

    # Insert data into comment_dimension
    insert_query = f"INSERT INTO comment_dimension ({', '.join(header)}) VALUES ({', '.join(['?'] * len(header))})"
    for row in csv_reader:
        cursor.execute(insert_query, row)
        cnxn.commit()

    print("Data from CSV file successfully inserted into comment_dimension.")
# ////


# Modify fact_table user_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ALTER COLUMN [user_id] INT NOT NULL;
"""
)
cnxn.commit()

# Modify user_dimension user_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[user_dimension]
    ALTER COLUMN [user_id] INT NOT NULL;
"""
)
cnxn.commit()

# Add primary key constraint to user_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[user_dimension]
    ADD CONSTRAINT PK_UserID PRIMARY KEY ([user_id]);
"""
)
cnxn.commit()

# Add foreign key constraint to fact_table referencing user_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ADD CONSTRAINT [FK_User]
    FOREIGN KEY ([user_id])
    REFERENCES [test].[dbo].[user_dimension] ([user_id]);
"""
)
cnxn.commit()

# Modify fact_table product_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ALTER COLUMN [product_id] INT NOT NULL;
"""
)
cnxn.commit()

# Modify product_dimension product_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[product_dimension]
    ALTER COLUMN [product_id] INT NOT NULL;
"""
)
cnxn.commit()

# Add primary key constraint to product_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[product_dimension]
    ADD CONSTRAINT PK_ProductID PRIMARY KEY ([product_id]);
"""
)
cnxn.commit()

# Add foreign key constraint to fact_table referencing product_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ADD CONSTRAINT [FK_Product]
    FOREIGN KEY ([product_id])
    REFERENCES [test].[dbo].[product_dimension] ([product_id]);
"""
)
cnxn.commit()


# Modify fact_table comment_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ALTER COLUMN [comment_id] INT NOT NULL;
"""
)
cnxn.commit()

# Modify comment_dimension comment_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[comment_dimension]
    ALTER COLUMN [comment_id] INT NOT NULL;
"""
)
cnxn.commit()

# Add primary key constraint to comment_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[comment_dimension]
    ADD CONSTRAINT PK_CommentID PRIMARY KEY ([comment_id]);
"""
)
cnxn.commit()

# Add foreign key constraint to fact_table referencing comment_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ADD CONSTRAINT [FK_Comment]
    FOREIGN KEY ([comment_id])
    REFERENCES [test].[dbo].[comment_dimension] ([comment_id]);
"""
)
cnxn.commit()

# Modify fact_table date_purchase_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ALTER COLUMN [date_purchase_id] INT NOT NULL;
"""
)
cnxn.commit()

# Modify date_dimension date_purchase_id column
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[date_dimension]
    ALTER COLUMN [date_purchase_id] INT NOT NULL;
"""
)
cnxn.commit()

# Add primary key constraint to date_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[date_dimension]
    ADD CONSTRAINT PK_DateID PRIMARY KEY ([date_purchase_id]);
"""
)
cnxn.commit()

# Add foreign key constraint to fact_table referencing date_dimension
cursor.execute(
    """
    ALTER TABLE [test].[dbo].[fact_table]
    ADD CONSTRAINT [FK_Date]
    FOREIGN KEY ([date_purchase_id])
    REFERENCES [test].[dbo].[date_dimension] ([date_purchase_id]);
"""
)
cnxn.commit()

# Create index on user_dimension
cursor.execute(
    """
    CREATE INDEX idx_user_id ON [test].[dbo].[user_dimension] (user_id);
"""
)
cnxn.commit()
print("Index created on user_dimension.")

# Create index on product_dimension
cursor.execute(
    """
    CREATE INDEX idx_product_id ON [test].[dbo].[product_dimension] (product_id);
"""
)
cnxn.commit()
print("Index created on product_dimension.")

# Create index on comment_dimension
cursor.execute(
    """
    CREATE INDEX idx_comment_id ON [test].[dbo].[comment_dimension] (comment_id);
"""
)
cnxn.commit()
print("Index created on comment_dimension.")

# Create index on date_dimension
cursor.execute(
    """
    CREATE INDEX idx_date_purchase_id ON [test].[dbo].[date_dimension] (date_purchase_id);
"""
)
cnxn.commit()
print("Index created on date_dimension.")

# Create a role for user management
cursor.execute('''
    CREATE ROLE UserManagement;
''')
cnxn.commit()
print("UserManagement role created.")

# Create a role for product management
cursor.execute('''
    CREATE ROLE ProductManagement;
''')
cnxn.commit()
print("ProductManagement role created.")

# Create a role for comment management
cursor.execute('''
    CREATE ROLE CommentManagement;
''')
cnxn.commit()
print("CommentManagement role created.")

# Create a role for date management
cursor.execute('''
    CREATE ROLE DateManagement;
''')
cnxn.commit()
print("DateManagement role created.")

# Grant necessary permissions to each role
# Example: Grant SELECT permission on a specific table to a role
cursor.execute('''
    GRANT SELECT ON [test].[dbo].[user_dimension] TO UserManagement;
''')
cnxn.commit()

# Close the cursor and connection
cursor.close()
cnxn.close()
