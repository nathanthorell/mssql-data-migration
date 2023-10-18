def get_conn_string(config, type):
    """
    Extract Source SQL Server credentials from config
    config expects json; type expects "source" or "destination"
    """
    server = config[f"{type}"]["sql_server"]
    db = config[f"{type}"]["database"]
    user = config[f"{type}"]["username"]
    password = config[f"{type}"]["password"]
    port = config[f"{type}"]["port"]
    driver = config[f"{type}"]["driver"]
    encrypt = config[f"{type}"]["encrypt"]

    conn = (
        f"DRIVER={driver};"
        f"SERVER={server},{port};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        "MARS_Connection=yes;"  # this is needed to handle Multiple Active Result Sets
    )
    return conn
