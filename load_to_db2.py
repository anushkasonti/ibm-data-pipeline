import jaydebeapi
import os
from dotenv import load_dotenv
from logger import table_info_logger  # Import the custom logger function

load_dotenv()


def load_to_db2(bucket_name, file_name, table_name):
    # JDBC connection details
    jdbc_url = (
        "jdbc:db2://6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud:30376/bludb:sslConnection=true;"
    )
    jdbc_driver = "com.ibm.db2.jcc.DB2Driver"

    # Load credentials from environment or dotenv
    jdbc_user = os.getenv("DB2_USER")
    jdbc_password = os.getenv("DB2_PASSWORD")

    # S3 credentials
    access_key_id = os.getenv("DB2_ACCESS_KEY_ID")
    secret_access_key = os.getenv("DB2_SECRET_ACCESS_KEY")

    # Connection setup using jaydebeapi
    connection = jaydebeapi.connect(jdbc_driver, jdbc_url, [jdbc_user, jdbc_password])

    try:
        # Load data using JDBC
        cursor = connection.cursor()

        # LOAD command
        load_command = (
            f"CALL SYSPROC.ADMIN_CMD('LOAD FROM \"S3::"
            f"s3.us-south.cloud-object-storage.appdomain.cloud::{access_key_id}::{secret_access_key}::{bucket_name}::{file_name}\""
            f" OF DEL REPLACE INTO {table_name}')"
        )
        # Log load command
        table_info_logger("Load Command", load_command)

        cursor.execute(load_command)

        # Log data loaded message
        table_info_logger("Data Load", "Data loaded into Db2")

        # # Sample query to verify the data
        # select_query = f"SELECT * FROM {table_name} LIMIT 2"
        # cursor.execute(select_query)
        # result = cursor.fetchall()
        #
        # # Log table info
        # table_info_logger(table_name, len(result))
        # Query to count total rows in the table
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        cursor.execute(count_query)
        total_rows = cursor.fetchone()[0]

        # Log table info
        table_info_logger(table_name, total_rows)

        # return result

    except Exception as e:
        # Log error message
        table_info_logger("Error", f"Error in loading data into {table_name}: {str(e)}")

    finally:
        # Close the connection
        connection.close()


if __name__ == "__main__":
    # Use dotenv for credentials
    bucket_name = "grp-stage-anushka"
    file_name = "CLOSED_STAGE"
    table_name = "CLOSED"

    load_to_db2(bucket_name, file_name, table_name)
