import sqlite3
from sqlite3 import Error


def load_tables(conn, table_insert):
    """
    Create a new tables from the create_tables_queries list
    If an error occurs, the error statement is printed

    Parameters:
    conn - connection to the sqlite database

    Returns:
    None
    """
    for df, table in table_insert:
        try:

            df.to_sql(table, conn, if_exists='replace', index=False)
        except Error as e:
            print(e)
