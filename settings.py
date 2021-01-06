import os

database_conn = {
    'username': os.getenv('DATABASE_USER', ''),
    'password': os.getenv('DATABASE_PASSWORD', ''),
    'host': os.getenv('DATABASE_HOST', ''),
    'port': os.getenv('DATABASE_PORT', 5432),
    'database': os.getenv('DATABASE_NAME', '')
}

if database_conn['host']:
    SQLALCHEMY_DATABASE_URI = 'postgresql://{username}:{password}' \
          '@{host}:{port}/{database}'.format(**database_conn)
else:
    path = os.getcwd()
    SQLALCHEMY_DATABASE_URI = 'sqlite:///{}/exabyte_test.db'.format(path)

is_prod = True if os.getenv("environment", "dev") == "prod" else False

DEBUG = not is_prod
