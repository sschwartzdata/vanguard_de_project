
def create_views(con, queries):
    """
    - Creates and connects to the spotify database
    - Returns the connection and cursor to spotify database
    """
    for query in queries:
        # connect to spotify database
        cur = con.cursor()
        cur.execute(query)
