'''
    @Author:Nagashree C R
    @Date: 11-08-2024
    @Last Modified by:Nagashree C R
    @Last Modified: 23-08-2024
    @Title : Menu Driven Crud operation

'''


import pyodbc
import os
import traceback
from dotenv import load_dotenv, dotenv_values

def create_database(sqlDbConn, database_name):
    """
        Description :
            this function create a database if it does not exist.
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database_name that you have to create
        Return:
            does not return anything
    """
     
    try:
        sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"CREATE DATABASE {database_name}")
        sqlDbConn.commit()
        print(f"Database '{database_name}' created successfully.")
    except Exception as e:
        print(f"database already exists.")  


def delete_database(sqlDbConn, database_name):

    """
        Description :
            this function delete a database if it exist.
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database_name that you have to create
        Return:
            does not return anything
    """

    try:
        sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"DROP DATABASE {database_name}")
        sqlDbConn.commit()
        print(f"Database '{database_name}' deleted successfully.")
    except Exception as e:
        print(f"database does not exists")


def create_table(sqlDbConn, database_name, table_name, table_structure):

    """
        Description :
            this function creates a table if it does not exist.
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            table_name : the table name that you have to create
            table_structure : the table structure in comma separated string. 
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        table_structure_list = [table_structure.split(",")]
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"CREATE TABLE {table_name} ({table_structure_list})")
        sqlDbConn.commit()
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"table already exists")


def read_table(sqlDbConn, database_name, table_name):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"select * from {table_name}")


        for row in cursor:
            print(f'{row}')

    except Exception as e:
        print(f"table does not exist.")  


def delete_table(sqlDbConn, database_name, table_name, condition):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"DROP TABLE {table_name}")
        sqlDbConn.commit()
        print("Deleted the table successfully")
    except Exception as e:
        print(f"table does not exist.")  


def delete_table_by_condition(sqlDbConn, database_name, table_name, condition):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"DELETE FROM {table_name} where {condition}")
        sqlDbConn.commit()
        print("Deleted the row successfully")

    except Exception as e:
        print(f"table does not exist. {e}")  


def insert_into_table(sqlDbConn, database_name, table_name, column_names, data_to_be_inserted):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        formatted_values = []
        for index, value in enumerate(data_to_be_inserted):
            # Enclose string values in quotes, integers are left as is
            if isinstance(value, str):
                formatted_values.append(f"'{value.strip()}'")
            else:
                formatted_values.append(str(value))

        # Convert lists to comma-separated strings
        columns_string = ", ".join(column_names)
        values_string = ", ".join(formatted_values)

        cursor.execute(f"insert into {table_name} ({columns_string}) VALUES ({values_string})")
        sqlDbConn.commit()
        print(f"Data inserted into table '{table_name}' successfully")

    except Exception as e:
        print(f"table does not exist.{e}")   


def get_schema(sqlDbConn, database_name, table_name):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        columns = cursor.fetchall()
        column_names = []
        for column in columns:
            column_names.append(column.COLUMN_NAME)
            
        return column_names
    except Exception as e:
        print(f"table does not exist.") 


def get_data_type(sqlDbConn, database_name, table_name):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        data_types = cursor.fetchall()
        data_types_names = []
        for column in data_types:
            data_types_names.append(column.DATA_TYPE)
            
        return data_types_names
    except Exception as e:
        print(f"table does not exist.") 


def update_table(sqlDbConn, database_name, table_name, column_name, column_value, condition):

    """
        Description :
            this function read data from a table
        Parameters :
            sqlDbConn: the connection string to connect with mssql
            database_name : the database name where we have to read the table
            table_name : the table name that we have to read
        Return:
            does not return anything
    """

    try:
        # sqlDbConn.autocommit = True
        cursor = sqlDbConn.cursor()
        cursor.execute(f"USE {database_name}")
        set_clause = []
        for col, val in zip(column_name, column_value):
            if isinstance(val, str):
                set_clause.append(f"{col} = '{val.strip()}'")
            else:
                set_clause.append(f"{col} = {val}")
        
        set_clause_string = ", ".join(set_clause)
        
        # Construct and execute the SQL update statement
        cursor.execute(f"UPDATE {table_name} SET {column_name} = {column_value} WHERE {condition}")

        sqlDbConn.commit()
        print("Updated the column successfully")
        
    except Exception as e:
        print(f"table does not exist.{e}") 


def main():
    load_dotenv()
    driver = os.getenv("Driver")
    server = os.getenv("Server")
    database = os.getenv("Database")
    trusted_connection = os.getenv("Trusted_Connection")

    pyodbc.drivers()
    connection_string = (
        "Driver={" + driver + "};"
        "Server=" + server + ";"
        
        "Trusted_Connection=" + trusted_connection + ";"
    )

    sqlDbConn = pyodbc.connect(connection_string)

    while True:
        try:
            print("_______CRUD OPERATION ON DATABASE_______")
            user_input = int(input(f"1. Create Database\n2. Create Table\n3. Read from Table\n4. Insert into Table\n5. Update Table\n6. Delete by condition\n7. Delete Table\n8. Delete Database\n9. exit\nEnter your choice: "))
            
            match(user_input):
                case 1:
                    database_name = input("Enter the database name: ")
                    create_database(sqlDbConn, database_name)
                case 2:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    table_structure = input("Enter the table structure in comma separated way: ")
                    create_table(sqlDbConn, database_name, table_name, table_structure)
                case 3:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    read_table(sqlDbConn, database_name, table_name)
                case 4:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    column_names = get_schema(sqlDbConn, database_name, table_name)
                    print(f"The columns in the table {table_name} are: {column_names}")
                    data_to_be_inserted_string = input("Enter the data that you want to insert in the table : ")
                    data_to_be_inserted = data_to_be_inserted_string.split(",")
                    data_types = get_data_type(sqlDbConn, database_name, table_name)
                    
                    for index in range(len(data_types)):
                        if data_types[index] == "int":
                            data = data_to_be_inserted[index].strip()
                            data_to_be_inserted[index] = int(data)

                    insert_into_table(sqlDbConn, database_name, table_name, column_names, data_to_be_inserted)
                case 5:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    column_names = get_schema(sqlDbConn, database_name, table_name)
                    print(f"The columns in the table {table_name} are: {column_names}")
                    print("DATA: ")
                    read_table(sqlDbConn, database_name, table_name)

                    column_name = input("Enter the column name you want to update (YOU CANT UPDATE PRIMARY KEY): ")
                    column_value = input("Enter the updated value that you want in your column: ")
                    condition = input("Enter the condition based on what you want to  update: ")

                    update_table(sqlDbConn, database_name, table_name, column_name, column_value, condition)
                    read_table(sqlDbConn, database_name, table_name)

                case 6:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    column_names = get_schema(sqlDbConn, database_name, table_name)
                    print(f"The columns in the table {table_name} are: {column_names}")
                    print("DATA: ")
                    read_table(sqlDbConn, database_name, table_name)

                    # column_name = input("Enter the column name you want to update (YOU CANT UPDATE PRIMARY KEY): ")
                    # column_value = input("Enter the updated value that you want in your column: ")
                    condition = input("Enter the condition based on what you want to  delete: ")

                    delete_table_by_condition(sqlDbConn, database_name, table_name, condition)
                    read_table(sqlDbConn, database_name, table_name)
                case 7:
                    database_name = input("Enter the database name: ")
                    table_name = input("Enter the table name: ")
                    delete_table(sqlDbConn, database_name, table_name)
                case 8:
                    database_name = input("Enter the database name: ")
                    delete_database(sqlDbConn, database_name)
                case 9:
                    print("\nThank you !!")
                    exit()
                case _:
                    print("Invalid choice. Please select a correct choice.")
        except ValueError:
            print("try giving an integer as an input.")
        except Exception as e:
            traceback.print_exc()  


if __name__ == "__main__":
    main()