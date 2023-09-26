def get_conn(config, type):
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

    conn = (
        f"DRIVER={driver};"
        f"SERVER={server},{port};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt=no"
    )
    return conn
