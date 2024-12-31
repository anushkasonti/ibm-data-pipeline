import datetime
import data_ingestion
import day1_create_dim_active
import day2_update_dim_active

def log_to_file(message):
    with open(r'C:\Users\anush\PycharmProjects\ibm_new2\log_file.log', 'a') as file:
        file.write(f'{datetime.datetime.now()} - {message}\n')

if __name__ == "__main__":
    log_to_file('Data pipeline started')

    log_to_file('Data ingestion started')
    data_ingestion.main(path_to_source_file="ACTIVE.CSV", table_name="ACTIVE")
    data_ingestion.main(path_to_source_file="CLOSED.CSV", table_name="CLOSED")
    log_to_file('Data ingestion completed')

    log_to_file('Day1 processing started')
    day1_create_dim_active.main()
    log_to_file('Day1 processing completed')

    log_to_file('Day2 processing started')
    day2_update_dim_active.main()
    log_to_file('Day2 processing completed')

    log_to_file('Data pipeline completed')


