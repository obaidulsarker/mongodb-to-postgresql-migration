from dotenv import load_dotenv
from pathlib import Path
import os
import platform

class EnvVariables:
    def __init__(self):

        dotenv_path = None
        os_name = platform.system()

        # if (os_name=="Windows"):
        #     dotenv_path = Path('cred\.env')
        # else:
        #     dotenv_path = Path('cred/.env')

        dotenv_path = os.path.join("cred", ".env")

        #dotenv_path = '.env'

        load_dotenv(dotenv_path=dotenv_path)

        self.MONGODB_HOST = os.getenv("MONGODB_HOST")
        self.MONGODB_PORT = os.getenv("MONGODB_PORT")
        self.MONGODB_USER = os.getenv("MONGODB_USER")
        self.MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD")

        self.IS_MONGODB_ATLAS = os.getenv("IS_MONGODB_ATLAS")
        self.MONGODB_URL = os.getenv("MONGODB_URL")

        self.BATCH_SIZE = os.getenv("BATCH_SIZE")

        # TARGET DB
        self.PG_HOST = os.getenv("PG_HOST")
        self.PG_PORT = os.getenv("PG_PORT")
        self.PG_DATABASE = os.getenv("PG_DATABASE")
        self.PG_USER = os.getenv("PG_USER")
        self.PG_PASSWORD = os.getenv("PG_PASSWORD")
        self.PG_SCHEMA = os.getenv("PG_SCHEMA")

        # FOR ETL
        self.ETL_PG_HOST = os.getenv("ETL_PG_HOST")
        self.ETL_PG_PORT = os.getenv("ETL_PG_PORT")
        self.ETL_PG_DATABASE = os.getenv("ETL_PG_DATABASE")
        self.ETL_PG_USER = os.getenv("ETL_PG_USER")
        self.ETL_PG_PASSWORD = os.getenv("ETL_PG_PASSWORD")
        self.ETL_PG_SCHEMA = os.getenv("ETL_PG_SCHEMA")
        self.ETL_PG_TABLE = os.getenv("ETL_PG_TABLE")

        self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY")
        self.LOG_FILE = os.getenv("LOG_FILE")
        self.PID_FILE = os.getenv("PID_FILE")
        self.INDEXES_XML_FILE_PATH=os.getenv("INDEXES_XML_FILE_PATH")

        # if (os_name=="Windows"):
        #     self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("/", "\\")
        #     self.LOG_FILE = os.getenv("LOG_FILE").replace("/", "\\")
        #     self.PID_FILE = os.getenv("PID_FILE").replace("/", "\\")
        # else:
        #     self.LOG_DIRECTORY = os.getenv("LOG_DIRECTORY").replace("\\", "/")
        #     self.LOG_FILE = os.getenv("LOG_FILE").replace("\\", "/")
        #     self.PID_FILE = os.getenv("PID_FILE").replace("\\", "/")
        
def get_variables():
    try:
        env_variable = EnvVariables()

        return env_variable
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    VARIABLES = EnvVariables()
    print(f"MONGODB Host = {VARIABLES.MONGODB_HOST}")
   