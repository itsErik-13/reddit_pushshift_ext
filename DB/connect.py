from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

def database_connect(database_name: str ):
    """
        Connect with mysql database

        Args:
            database_name (str): Database name.

        Returns:
            session: sqlalchemy object
    """

    engine = create_engine(
        f"mysql+mysqlconnector://root:root@localhost:3306/{database_name}?charset=utf8mb4"
    )

    Session = sessionmaker(bind=engine, autoflush=False)

    return Session()

def database_engine(database_name: str ):
    """
        Connect with mysql database

        Args:
            database_name (str): Database name.

        Returns:
            engine: engine sqlalchemy object
    """

    engine = create_engine(
        f"mysql+mysqlconnector://root:root@localhost:3306/{database_name}?charset=utf8mb4"
    )

    return engine