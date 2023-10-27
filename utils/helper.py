import psycopg2

def create_dbconnection(host, user, password, database):
    """
    This function creates a connection to the weserve database in PostgreSQL.

    Parameters:
    - host (str): The hostname or IP address of the database server.
    - user (str): The username for the database.
    - password (str): The password for the database user.
    - database (str): The name of the database to connect to.

    Returns:
    psycopg2.extensions.connection: A connection object to the PostgreSQL database.
    """
    return psycopg2.connect(
        user=user,
        host=host,
        password=password,
        database=database
    )

      

