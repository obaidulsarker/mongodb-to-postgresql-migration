from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime, timedelta
from datetime_truncate import truncate
from logger import *
from setting import get_variables
import pandas as pd

class MongoDatabase(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)
        self.host = get_variables().MONGODB_HOST
        self.port = get_variables().MONGODB_PORT
        self.username = get_variables().MONGODB_USER
        self.password = get_variables().MONGODB_PASSWORD

        self.is_mongodb_atlas = get_variables().IS_MONGODB_ATLAS
        self.mongodb_url = get_variables().MONGODB_URL

        self.batch_size = get_variables().BATCH_SIZE

        self.archive_connection=None
        self.source_connection=None

    # Connection method
    def source_connect(self):
        try:

            self.source_connection = MongoClient()

            # Construct the MongoDB connection URI
            if (self.is_mongodb_atlas=="NO"):
                self.source_connection = MongoClient(f"mongodb://{self.host}:{self.port}/", username=self.username, password=self.password)
            else:
                self.source_connection = MongoClient(self.mongodb_url)

            return self.source_connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Connection method
    def source_db_connect(self, db_name):
        try:
            
            self.source_connection = MongoClient()

            # Construct the MongoDB connection URI
            if (self.is_mongodb_atlas=="NO"):
                self.source_connection = MongoClient(f"mongodb://{self.host}:{self.port}/", username=self.username, password=self.password)
            else:
                self.source_connection = MongoClient(self.mongodb_url)

            db = self.source_connection[db_name]

            return db  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Connection method for archive database
    def archive_connect(self):
        try:

            self.archive_connection = MongoClient(f"mongodb://{self.host_archive}:{self.port_archive}/", username=self.username_archive, password=self.password_archive)

            return self.archive_connection  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
        
    # Connection method for archive database
    def archive_db_connect(self, db_name):
        try:

            self.archive_connection = MongoClient(f"mongodb://{self.host_archive}:{self.port_archive}/", username=self.username_archive, password=self.password_archive)

            db = self.archive_connection[db_name]

            return db  # Success
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Disconnect client
    def db_disconnect(self):
        try:
            if self.source_connection: #Check if the client is initialized
                self.source_connection.close()
                self.source_connection = None # Set client to None after closing

            if self.archive_connection: #Check if the client is initialized
                self.archive_connection.close()
                self.archive_connection = None # Set client to None after closing

        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Create Index in source database
    def create_index(self, database_name, collection_name, field_name):
        try:
            
             #Connection 
            connection = self.source_connect()
            db=connection[database_name]
            collection = db[collection_name]

            # Get a list of existing indexes
            existing_indexes = collection.index_information()

            # Check if the field name already exists in any existing indexes
            if any(field_name in index['key'] for index in existing_indexes.values()):
                # Check if field_name exists in any existing index
                for index_name, index in existing_indexes.items():
                    if field_name in index['key']:
                        print(f"Index '{index_name}' already exists for field '{field_name}'.")
                        self.log_info(f"Index '{index_name}' already exists for field '{field_name}'.")
                        return index_name
            else:
                # Create the index
                index_name = collection.create_index([(field_name, ASCENDING)])
                print(f"Index for field '{field_name}' created successfully.")
                self.log_info(f"Index for field '{field_name}' created successfully.")

            return index_name
    
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error

    # Get record counts
    def get_records_count(self, db_connection, collection_name, filter_criteria):
        total_docs = 0
        try:
            # connection
            collection = db_connection[collection_name]

            # Aggregation pipeline
            pipeline = [
                    {"$match": filter_criteria},  # Match documents based on filter criteria
                    {"$group": {
                        "_id": None,
                        "count": {"$sum": 1}
                    }},
                    {"$project": {
                        "_id": 0,
                        "count": 1
                    }}
                    ]

            # Execute the aggregation pipeline
            result = list(collection.aggregate(pipeline))

            if result:
                total_docs = result[0]["count"]
            else:
                total_docs = 0

            return total_docs
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return total_docs  # Error
    
    # Compact Collection
    def compact_collection(self, db_connection, collection_name):
        try:
            #collection = db_connection[collection_name]
            result = db_connection.command('compact', collection_name)

            print("Compact operation completed.")
            self.log_info("Compact operation completed.")
            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Compact Database
    def compact_database(self, db_connection):
        try:
           
            result = db_connection.command('compact')
            print("Compact operation completed.")
            self.log_info("Compact operation completed.")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Repair Database
    def repair_database(self, db_connection):
        try:

            result = db_connection.command('repairDatabase')
            print("Repair operation completed.")
            self.log_info("Repair operation completed.")

            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            return None  # Error
    
    # Validate fields
    def validate_fields(self, database_name, collection_name, fields_str):
        """
        Validates the existence of a list of fields in a MongoDB collection.

        Args:
            database_name (str): The name of the MongoDB database.
            collection_name (str): The name of the MongoDB collection.
            fields_str (str): A comma-separated string of field names to validate.
            connection_string (str, optional): The MongoDB connection string. Defaults to "mongodb://localhost:27017/".

        Returns:
            list: A list of invalid field names, or an empty list if all fields are valid.
        """
        try:
            #Connection 
            connection = self.source_connect()
            db=connection[database_name]
            collection = db[collection_name]

            fields = [field.strip() for field in fields_str.split(',')]
            invalid_fields = []

            # Get a sample document to check field existence.
            sample_document = collection.find_one()

            if sample_document:
                existing_fields = sample_document.keys()

                for field in fields:
                    if field not in existing_fields:
                        invalid_fields.append(field)

            else:
                #If collection is empty, then all provided fields are considered invalid.
                invalid_fields.extend(fields)

            return invalid_fields

        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return fields # return all fields in case of error.

        finally:
            if 'connection' in locals() and connection:
                connection.close()


    def list_to_comma_string(self, input_list):
        """Converts a list into a comma-separated string.

        Args:
            input_list: The list to convert.

        Returns:
            A string with the list elements separated by commas.
        """
        return ', '.join(map(str, input_list))

    def is_validate_fields(self, database_name, collection_name, fields_str):
        try:
            is_valid = False

            # Validate fileds
            invalid_fields = self.validate_fields(database_name=database_name, collection_name=collection_name, fields_str=fields_str)

            if len(invalid_fields)>0:
                is_valid = False
                raise Exception(f"Invalid Fields in {collection_name}: {self.list_to_comma_string(invalid_fields)}")
            else:
                is_valid = True

            return is_valid

        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return is_valid

    # Get Maximum, Minimum and Total documents
    def get_minDate_maxDate_total_docs(self, database_name, collection_name, timestamp_column, from_date=None):
        try:
            max_date = None
            min_date = from_date
            total_docs = 0

            #Connection 
            connection = self.source_connect()
            db=connection[database_name]
            collection = db[collection_name]

            # Query
            filter_query = {f"{timestamp_column}": {"$gte": min_date}}
            print(filter_query)

            min_date_doc = collection.find_one(filter_query, sort=[(f"{timestamp_column}", ASCENDING)])
            max_date_doc = collection.find_one(filter_query, sort=[(f"{timestamp_column}", DESCENDING)])
            total_docs = collection.count_documents(filter_query)

            if min_date_doc and max_date_doc:
                min_date = min_date_doc[f"{timestamp_column}"]
                max_date =  max_date_doc[f"{timestamp_column}"]
            else:
                total_docs=0
                print("No documents match the filter criteria.")
                self.log_info("No documents match the filter criteria.")

            
            return min_date, max_date, total_docs
        
        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return None, None, None

    # Return database of a query
    def get_dataframe(self, database_name, collection_name, fields_str, timestamp_column, from_date=None, to_date=None, row_limit=1000):
        """
        Retrieves records from a MongoDB collection and returns them as a Pandas DataFrame.

        Args:
            db_name (str): The name of the MongoDB database.
            collection_name (str): The name of the MongoDB collection.
            fields_str (str): A comma-separated string of field names to retrieve.
            created_timestamp (datetime, optional): An optional datetime object specifying the 
                                                    minimum 'created_at' timestamp for filtering. Defaults to None.
        Returns:
            pandas.DataFrame: A DataFrame containing the retrieved records, or an empty DataFrame 
                            if an error occurs or no records are found.
        """
        try:

            #Connection 
            connection = self.source_connect()
            db=connection[database_name]
            collection = db[collection_name]
            
            fields = [field.strip() for field in fields_str.split(',')]
            projection = {field: 1 for field in fields}
            #projection["_id"] = 0  # Exclude the _id field from the results

            
            query = {}
            if timestamp_column:
                #query[f"{timestamp_column}"] = {"$gte": timestamp_value}
                #filter_query = {f"{timestamp_column}": {"$lt": timestamp_value}}
                if (from_date and to_date):
                    query = {
                        f"{timestamp_column}": { "$gte": from_date,"$lte": to_date}
                    }
                elif (from_date):
                    query = {f"{timestamp_column}": {"$gte": from_date}}
                elif (to_date):
                    query = {f"{timestamp_column}": {"$lte": to_date}}
            
            print(f"Mongo Query: {query}")
            self.log_info(f"Mongo Query: {query}")

            cursor = None
            if row_limit:
                cursor = collection.find(query, projection).limit(row_limit)
            else:
                cursor = collection.find(query, projection)

            df = pd.DataFrame(list(cursor))
            
            return df

        except Exception as e:
            print(f"An error occurred: {e}")
            self.log_error(f"An error occurred: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of error
        
        finally:
            if 'connection' in locals() and connection:
                connection.close() #close the connection.
                
    # Archive Collection        
    def archive_data(self, source_db_name, source_collection_name, archive_db_name, archive_collection_name, ts_field_name):
        try:

            total_archived = 0
            total_deleted = 0

            # Connection 
            source_connection = self.source_connect()
            archive_connection=self.archive_connect()

            # DB Connection
            source_db_conn=source_connection[source_db_name]
            archive_db_conn=archive_connection[archive_db_name]

            # source_db_conn=self.source_connection[source_db_name]
            # archive_db_conn=self.archive_connection[archive_db_name]

            # Create index on source DB
            index_name = self.create_index(source_db_connection=source_db_conn, collection_name=source_collection_name, field_name=ts_field_name)
            
            if not index_name:
                raise Exception("Unable to create index.")
            
            # Find cutoff date
            cutoff_date = datetime.now() - timedelta(days=self.data_retention_days)
            cutoff_date = truncate(cutoff_date, 'day')

            print(f"Data Retention From: {cutoff_date}")
            self.log_info(f"Data Retention From: {cutoff_date}")

            # Query
            filter_query = {f"{ts_field_name}": {"$lt": cutoff_date}}
            print(filter_query)

            # # Aggregation pipeline
            # pipeline = [
            #             {"$match": filter_query},  # Match documents based on filter criteria
            #             {"$group": {
            #                 "_id": None,
            #                 "min_date": {"$min": f"${ts_field_name}"},
            #                 "max_date": {"$max": f"${ts_field_name}"},
            #                 "count": {"$sum": 1}
            #             }},
            #             {"$project": {
            #                 "_id": 0,
            #                 "min_date": 1,
            #                 "max_date": 1,
            #                 "count": 1
            #             }}
            #             ]

            # Connect to collection
            source_collection = source_db_conn[source_collection_name]
            archive_collection = archive_db_conn[archive_collection_name]

            from_date = truncate(datetime.now(), 'day')
            to_date = from_date
            min_date_doc = source_collection.find_one(filter_query, sort=[(f"{ts_field_name}", ASCENDING)])
            max_date_doc = source_collection.find_one(filter_query, sort=[(f"{ts_field_name}", DESCENDING)])
            total_docs = source_collection.count_documents(filter_query)
            if min_date_doc and max_date_doc:
                from_date = min_date_doc[f"{ts_field_name}"]
                to_date =  max_date_doc[f"{ts_field_name}"]
            else:
                total_docs=0
                from_date = to_date + timedelta(days=1)
                print("No documents match the filter criteria.")
                self.log_info("No documents match the filter criteria.")
                return total_archived, total_deleted
            
            from_date = min_date_doc[f"{ts_field_name}"] if min_date_doc else None
            to_date = max_date_doc[f"{ts_field_name}"] if max_date_doc else None

            # # Execute the aggregation pipeline
            # result = list(source_collection.aggregate(pipeline))
            # from_date = truncate(datetime.now(), 'day')
            # to_date = from_date

            # if result:
            #     from_date = result[0]['min_date']
            #     to_date = result[0]['max_date']
            #     total_docs = result[0]["count"]

            # else:
            #     from_date = to_date + timedelta(days=1)
            #     total_docs = 0
            #     print("No documents match the filter criteria.")
            #     self.log_info("No documents match the filter criteria.")
            
            from_date = truncate(from_date, 'day')
            to_date = truncate(to_date, 'day')

            print(f"Total records for archive: {total_docs}")
            print(f"Archive Start Date: {from_date}")
            print(f"Archive End Date: {to_date}")

            self.log_info(f"Total records for archive: {total_docs}")
            self.log_info(f"Archive Start Date: {from_date}")
            self.log_info(f"Archive End Date: {to_date}")

            while (from_date<=to_date):

                start_date = from_date
                end_date = start_date + timedelta(days=1)
                # iso_start_date_str = start_date.isoformat()
                # iso_end_date_str = end_date.isoformat()

                filter_query_datewise = {
                    f"{ts_field_name}": {
                        "$gte": start_date,
                        "$lt": end_date
                    }
                }
                while True:
                    # Read documents for archive
                    batch = []
                    deleted_count= 0
                    batch = list(source_collection.find(filter_query_datewise).limit(self.batch_size))

                    inserted_id_lst = []

                    if batch:
                        try:
                            try:
                                archive_result = archive_collection.insert_many(batch, ordered=False, bypass_document_validation=True)
                                if archive_result.acknowledged:
                                    total_archived+=len(archive_result.inserted_ids)
                                    inserted_id_lst.extend(archive_result.inserted_ids)

                            except Exception as e:
                                self.log_error(f"Exception: {str(e)}")
                                print(f"Exception: {str(e)}")
                                # If error occured in bulk insert, then insert one by one using upsert
                                for document in batch:
                                    archive_result_replace = archive_collection.replace_one({"_id": document["_id"]}, document, upsert=True)
                                    if archive_result_replace.acknowledged:
                                        total_archived += 1
                                        inserted_id_lst.extend(archive_result_replace.upserted_id)
                            
                            # Delete archived data from source collection after successful archiving
                            # deleted_result = source_collection.delete_many({"_id": {"$in": [doc["_id"] for doc in batch]}}, comment="Archiving records")
                            if inserted_id_lst:
                                deleted_result = source_collection.delete_many({"_id": {"$in": inserted_id_lst}})
                                if deleted_result.acknowledged:
                                    deleted_count = deleted_result.deleted_count
                                    total_deleted += deleted_count
    
                        except Exception as e:
                            print(f"Error deleting archived documents from source: {e}")
                            self.log_error(f"Error deleting archived documents from source: {e}")
                            #break
                    
                    # Exit loop if record does not exist
                    if deleted_count == 0:
                        break

                str_date=from_date.strftime("%Y-%m-%d")

                print(f"{source_db_name}->{source_collection_name} - Archived[{str_date}]: {total_archived}/{total_docs}")
                self.log_info(f"{source_db_name}->{source_collection_name} - Archived[{str_date}]: {total_archived}/{total_docs}")

                print(f"{source_db_name}->{source_collection_name} - Deleted[{str_date}]: {total_deleted}/{total_docs}")
                self.log_info(f"{source_db_name}->{source_collection_name} - Deleted[{str_date}]: {total_deleted}/{total_docs}")

                # Next date
                from_date = from_date + timedelta(days=1)
            
            # Compact collection
            compact_status=self.compact_collection(db_connection=source_db_conn, collection_name=source_collection_name)
            

            self.db_disconnect()
            
            return total_archived, total_deleted

        except Exception as e:
            self.db_disconnect()
            print(f"Error: {e}")
            self.log_error(f"Exception: {str(e)}")
            print(f"Total Archived Documents: {total_archived}")
            self.log_info(f"Total Archived Documents: {total_archived}")
            return total_archived, total_deleted
        

# if __name__ == "__main__":
#     # Replace with your MongoDB connection details
#     source_uri = "mongodb://felrnd:'tNdjhuf78sv'@192.168.169.91:27017/"  # Source MongoDB URI
#     archive_uri = "mongodb://felrnd:'tNdjhuf78sv'@192.168.169.91:27018/" # Archive MongoDB URI
#     source_db_name = "foodi-payment-service"
#     source_collection_name = "payments"
#     archive_db_name = "foodi-payment-service"
#     archive_collection_name = "payments"
#     batch_size = 1000

#     archiver = MongoArchiver(source_uri, archive_uri, source_db_name, source_collection_name, archive_db_name, archive_collection_name, batch_size)
#     archiver.archive_data(days=30)
#     archiver.close_connections()
#     print("Archiving process completed.")