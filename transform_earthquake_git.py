import psycopg2
import os

def delete_old_records(conn, start_date, end_date):

    delete_query = """        DELETE FROM postgres.public.raw_earthquake
    WHERE dt BETWEEN %s AND %s
    """
    cur = conn.cursor()
    cur.execute(delete_query, (start_date, end_date))
    conn.commit()
    cur.close()


def transform_earthquake(conn, start_date, end_date):

    cur = conn.cursor()
    sql = """
        INSERT INTO postgres.public.raw_earthquake
        SELECT
            to_timestamp(CAST(ts AS bigint)/1000) AS dt,
            ts,
            TRIM(SUBSTRING(place FROM POSITION(' of ' IN place) + 3)) AS place,
            magnitude,
            latitude,
            longitude,
            depth
        FROM postgres.public.earthquake
        WHERE to_timestamp(CAST(ts AS bigint)/1000)::date BETWEEN %s AND %s
    """
    cur.execute(sql, (start_date, end_date))
    conn.commit()
    cur.close()

def main():

    # Database connection parameters
    db_params = {
        "dbname": os.getenv("variable_1"),
        "user": os.getenv("variable_2"),
        "password": os.getenv("variable_3"),
        "host": os.getenv("variable_4"),
        "port": os.getenv("variable_5"),
    }

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(**db_params)

    start_date = "2025-05-28"
    end_date = "2025-06-04"

    # Delete old records
    delete_old_records(conn, start_date, end_date)

    # Load data into the table
    transform_earthquake(conn, start_date, end_date)
    print("Data loaded successfully")

    conn.close()


if __name__ == "__main__":
    main()