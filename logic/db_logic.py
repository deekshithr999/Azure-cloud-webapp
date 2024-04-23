
# Improved configuration with error handling
config = {
    'user': 'sfengrimyo',
    'password': 'Anewpswd9',
    'host': 'ccwebapp12-server.mysql.database.azure.com',
    'port': 3306,
    'database': 'ccprojectdb',
    'ssl_disabled': True,
    'raise_on_warnings': True
}



# Replace these variables with your Azure MySQL server details
host = 'ccwebapp12-server.mysql.database.azure.com'
username = 'sfengrimyo'
password = 'Anewpswd9'
database = 'ccprojectdb'


import mysql.connector
import csv
import pandas as pd
import plotly.express as px
import plotly.io as pio

def connect_to_database():
    """Establishes a db to the MySQL database."""
    try:
        db = mysql.connector.connect(
            host=host,
            user=username,
            password=password,
            database=database
        )
        print("Connected to the database successfully.")
        return db
    except mysql.connector.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_cursor(db):
    # Create a cursor object
    cursor = db.cursor()
    return cursor



def create_tables(db):
    """Creates tables in the database."""
    if db is None:
        print("No db to the database.")
        return
    
    cursor = db.cursor()

    # Define SQL commands to create tables
    create_products_table = """
    CREATE TABLE IF NOT EXISTS products (
        PRODUCT_NUM INT PRIMARY KEY,
        DEPARTMENT VARCHAR(255),
        COMMODITY VARCHAR(255),
        BRAND_TYPE VARCHAR(255),
        NATURAL_ORGANIC_FLAG VARCHAR(10)
    )
    """

    create_transactions_table = """
    CREATE TABLE IF NOT EXISTS transactions (
        BASKET_NUM INT,
        HSHD_NUM INT,
        PURCHASE_DATE VARCHAR(255),
        PRODUCT_NUM INT,
        SPEND DECIMAL(10, 2),
        UNITS INT,
        STORE_REGION VARCHAR(255),
        WEEK_NUM INT,
        YEAR VARCHAR(255)
    )
    """

    create_households_table = """
    CREATE TABLE IF NOT EXISTS households (
        HSHD_NUM INT PRIMARY KEY,
        LOYALTY_FLAG CHAR(1),
        AGE_RANGE VARCHAR(255),
        MARITAL_STATUS VARCHAR(255),
        INCOME_RANGE VARCHAR(255),
        HOMEOWNER_DESC VARCHAR(255),
        HSHD_COMPOSITION VARCHAR(255),
        HSHH_SIZE VARCHAR(255),
        CHILDREN VARCHAR(100)
    )
    """

    # Execute SQL commands to create tables
    try:
        cursor.execute(create_products_table)
        cursor.execute(create_transactions_table)
        cursor.execute(create_households_table)
        db.commit()
        print("Tables created successfully.")
    except mysql.connector.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        cursor.close()




def process_file(file, table_name, db):
    try:
        cursor = db.cursor()
        cursor.execute(f"TRUNCATE TABLE {table_name}")  # Truncate table to remove existing data
        
        # Open the file in text mode
        with file.stream as stream:
            reader = csv.reader(stream.read().decode("utf-8").splitlines())
            next(reader)  # Skip the header row
            for row in reader:
                cursor.execute(f"INSERT INTO {table_name} VALUES (%s)" % ', '.join(['%s'] * len(row)), row)
        
        db.commit()
        print(f" {file} File processed successfully.")
    except mysql.connector.Error as e:
        db.rollback()
        print(f"Error processing file {file}: {e}")
    finally:
        cursor.close()


def upload_files(files, db):
    file1, file2, file3 = files
    
    process_file(file1, 'households', db)
    process_file(file2, 'products', db)
    process_file(file3, 'transactions', db)

    return "Files uploaded and processed successfully"


#q3
def standard_tables_display(db):
    cursor = db.cursor()
    # cursor.execute("""
    #     SELECT h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    #     FROM households h
    #     JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
    #     JOIN products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
    #     WHERE h.HSHD_NUM = 4904
    #     ORDER BY h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    # """)

    cursor.execute("""
        SELECT *
        FROM householdsfull h
        JOIN transactionsfull t ON h.HSHD_NUM = t.HSHD_NUM
        JOIN productsfull p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        WHERE h.HSHD_NUM = 10
        ORDER BY h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    """)
    headers = [desc[0] for desc in cursor.description]
    # Fetch all results
    results = cursor.fetchall()

    # Close cursor and connection
    cursor.close()
    return headers, results


def dynamic_tables_display(db, hshd_num):
    cursor = db.cursor()
    cursor.execute("""
        SELECT *
        FROM households h
        JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
        JOIN products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        WHERE h.HSHD_NUM = %s
        ORDER BY h.HSHD_NUM, t.BASKET_NUM, t.PURCHASE_DATE, t.PRODUCT_NUM, p.DEPARTMENT, p.COMMODITY
    """, (hshd_num,))
    headers = [desc[0] for desc in cursor.description]
    # Fetch all results
    results = cursor.fetchall()

    # Close cursor and connection
    cursor.close()
    return headers, results



# Function to query data from the database
def query_data(query, db):
    cursor = db.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

# Function to generate charts
def generate_charts(db):
    # Query data for household composition and income range
    query_household_income = """
    SELECT HSHD_COMPOSITION, INCOME_RANGE, SUM(SPEND) AS TOTAL_SPEND
    FROM transactions t
    INNER JOIN households h ON t.HSHD_NUM = h.HSHD_NUM
    GROUP BY HSHD_COMPOSITION, INCOME_RANGE
    """
    data_household_income = query_data(query_household_income, db)
    
    # Create DataFrame for household composition and income range
    df_household_income = pd.DataFrame(data_household_income, columns=['HSHD_COMPOSITION', 'INCOME_RANGE', 'TOTAL_SPEND'])

    # Generate chart for household composition and income range
    fig_household_income = px.bar(df_household_income, x='HSHD_COMPOSITION', y='TOTAL_SPEND', color='INCOME_RANGE',
                                   barmode='group', title='Total Spend by Household Composition and Income Range',width=1500,
                                   color_discrete_sequence=px.colors.qualitative.Set1)
    
    # Query data for presence of children and total spend
    query_children_spend = """
    SELECT CHILDREN, SUM(SPEND) AS TOTAL_SPEND
    FROM households h
    INNER JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
    GROUP BY CHILDREN
    """
    data_children_spend = query_data(query_children_spend, db)
    
    # Create DataFrame for presence of children and total spend
    df_children_spend = pd.DataFrame(data_children_spend, columns=['CHILDREN', 'TOTAL_SPEND'])
    
    # Generate chart for presence of children and total spend
    fig_children_spend = px.bar(df_children_spend, x='CHILDREN', y='TOTAL_SPEND', title='Total Spend by Presence of Children',
    color_discrete_sequence=px.colors.qualitative.Set2)
    
    # Query data for household size and total spend
    query_household_size_spend = """
    SELECT HSHH_SIZE, SUM(SPEND) AS TOTAL_SPEND
    FROM households h
    INNER JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
    GROUP BY HSHH_SIZE
    """
    data_household_size_spend = query_data(query_household_size_spend, db)
    
    # Create DataFrame for household size and total spend
    df_household_size_spend = pd.DataFrame(data_household_size_spend, columns=['HSHH_SIZE', 'TOTAL_SPEND'])
    
    # Generate chart for household size and total spend
    fig_household_size_spend = px.bar(df_household_size_spend, x='HSHH_SIZE', y='TOTAL_SPEND', title='Total Spend by Household Size', 
    color_discrete_sequence=px.colors.qualitative.Set3)

    # Query data for marital status and total spend
    query_marital_status_spend = """
    SELECT MARITAL_STATUS, SUM(SPEND) AS TOTAL_SPEND
    FROM households h
    INNER JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
    GROUP BY MARITAL_STATUS
    """
    data_marital_status_spend = query_data(query_marital_status_spend, db)
    
    # Create DataFrame for marital status and total spend
    df_marital_status_spend = pd.DataFrame(data_marital_status_spend, columns=['MARITAL_STATUS', 'TOTAL_SPEND'])
    
    # Generate chart for marital status and total spend
    fig_marital_status_spend = px.bar(df_marital_status_spend, x='MARITAL_STATUS', y='TOTAL_SPEND', title='Total Spend by Marital Status',
    color_discrete_sequence=px.colors.qualitative.Pastel1)

    # Query data for age range and total spend
    query_age_range_spend = """
    SELECT AGE_RANGE, SUM(SPEND) AS TOTAL_SPEND
    FROM households h
    INNER JOIN transactions t ON h.HSHD_NUM = t.HSHD_NUM
    GROUP BY AGE_RANGE
    """
    data_age_range_spend = query_data(query_age_range_spend, db)
    
    # Create DataFrame for age range and total spend
    df_age_range_spend = pd.DataFrame(data_age_range_spend, columns=['AGE_RANGE', 'TOTAL_SPEND'])
    
    # Generate chart for age range and total spend
    fig_age_range_spend = px.bar(df_age_range_spend, x='AGE_RANGE', y='TOTAL_SPEND', title='Total Spend by Age Range',
    color_discrete_sequence=px.colors.qualitative.Pastel2)

    # Add more charts here for other demographic factors...

    # Convert figures to HTML representations
    fig_household_income_html = pio.to_html(fig_household_income, full_html=False)
    fig_children_spend_html = pio.to_html(fig_children_spend, full_html=False)
    fig_household_size_spend_html = pio.to_html(fig_household_size_spend, full_html=False)
    fig_marital_status_spend_html = pio.to_html(fig_marital_status_spend, full_html=False)
    fig_age_range_spend_html = pio.to_html(fig_age_range_spend, full_html=False)

    # return fig_household_income, fig_children_spend, fig_household_size_spend, fig_marital_status_spend, fig_age_range_spend
    return fig_household_income_html, fig_children_spend_html, fig_household_size_spend_html, fig_marital_status_spend_html, fig_age_range_spend_html