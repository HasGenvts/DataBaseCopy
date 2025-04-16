from setuptools import setup, find_packages

setup(
    name="database-copy",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "loguru",
        "sqlalchemy",
        "pymysql",
        "psycopg2-binary",
        "pyodbc",
    ],
) 