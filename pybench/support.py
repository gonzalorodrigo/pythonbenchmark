import os
def get_pg_data(username=None, password=None):
    username = os.getenv("POSTGRES_USER", username)
    if not username:
        raise Exception("No username found")
    password = os.getenv("POSTGRES_PASSWORD", password)
    if not password:
        secret_file_name = os.getenv("POSTGRES_PASSWORD_FILE", None)
        if secret_file_name:
            with open(secret_file_name, "r") as f:
                password = f.read()
        if not password:
            raise Exception("No password found")
    return username, password