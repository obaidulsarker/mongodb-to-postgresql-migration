from PostgresqlDB import *
from MongoDatabase import *
from xml_reader import *

from logger import Logger

class ETL(Logger):
    def __init__(self, logfile, operation_id):
        super().__init__(logfile)
        self.operation_log=logfile
        self.operation_id = operation_id
        self.batch_size = get_variables().BATCH_SIZE

    # Doing automation tasks
    def start_jobs(self):
        try:
        
            operation_log = self.operation_log

            # Read all collections configuration info
            xml = XmlReader(logfile=operation_log)

            # PostgreSQL Instance
            pg = PostgresDatabase(logfile=operation_log)

            # MongoDB instance
            mongo = MongoDatabase(logfile=operation_log)
            
            # Loop through collection
            for item in xml.get_collection_list():
                try:

                    # Set Value
                    database_name=item.database_name
                    collection_name=item.collection_name
                    fields_str=item.fields_list
                    unique_column=item.id_field_name
                    timestamp_column=item.ts_field_name
                    pg_table_name=item.collection_name

                    # get last sync timestamp of the collection from ETL table
                    last_sync_ts=pg.get_last_sync_time(pg_table_name=item.collection_name)
                    if last_sync_ts is None:
                        last_sync_ts=datetime.now() - relativedelta(years=100)

                    str_last_sync_ts=last_sync_ts.strftime("%Y-%m-%d")

                    print(f"last sync date: {str_last_sync_ts}")
                    self.log_info(f"last sync date: {str_last_sync_ts}")

                    # Create Index on Timestamp Column
                    index_name = mongo.create_index(database_name=database_name, collection_name=collection_name, field_name=timestamp_column)
                    if not index_name:
                        raise Exception(f"Unable to create index on {collection_name} collection.")
            
                    # Validate MongoDB Collection Fields
                    has_valid_fields = mongo.is_validate_fields(collection_name=collection_name, database_name=database_name, fields_str=fields_str)
                    if (has_valid_fields is not True):
                        raise Exception(f"Invalid fields found in configuration for the {collection_name} collection.")

                    # Find Minimum and Maximum date of Timestamp column and total records
                    minimum_datetime, maximum_datetime, total_documents = mongo.get_minDate_maxDate_total_docs(database_name=database_name, collection_name=collection_name, timestamp_column=timestamp_column, from_date=last_sync_ts)
                    
                    print(f"Minimum value of {timestamp_column} field on {collection_name}: {minimum_datetime}")
                    print(f"Maximum value of {timestamp_column} field on {collection_name}: {maximum_datetime}")
                    print(f"Total records for synchronization of {collection_name}: {total_documents}")
                    
                    self.log_info(f"Minimum value of {timestamp_column} field on {collection_name}: {minimum_datetime}")
                    self.log_info(f"Maximum value of {timestamp_column} field on {collection_name}: {maximum_datetime}")
                    self.log_info(f"Total records for synchronization of {collection_name}: {total_documents}")

                    # Get dataframe of MongoDB
                    sample_mongo_data = mongo.get_dataframe(database_name=database_name, collection_name=collection_name, fields_str=fields_str, timestamp_column=timestamp_column, from_date=minimum_datetime, to_date=maximum_datetime, row_limit=500)
                    
                    if (sample_mongo_data.empty):
                        raise Exception(f"No record found in {collection_name}")

                    # Create PostgreSQL Table
                    table_created= pg.create_table_if_not_exists(df=sample_mongo_data, collection_info=item)

                    if table_created is None:
                        raise Exception(f"Unable to create {collection_name}")
                    

                    # # Add new columns in PostgreSQL
                    # created_new_columns= pg.alter_table_for_new_columns(mongo_df=mongo_df.head(), table_name=item.collection_name)

                    # if created_new_columns is None:
                    #     raise Exception(f"Unable alter table {item.collection_name}")
                    
                    from_date = truncate(minimum_datetime, 'day')
                    to_date = truncate(maximum_datetime, 'day')

                
                    record_count = 0
                    batch_record_count= 0


                    while (from_date<=to_date):
                        start_date = from_date
                        end_date = start_date + timedelta(days=1)
                        batch_record_count=0

                        # Next Date
                        from_date= end_date

                        str_date=start_date.strftime("%Y-%m-%d")
                        print(f"Sync data: {str_date}")
                        self.log_info(f"Sync data: {str_date}")

                        batch = mongo.get_dataframe(database_name=database_name, collection_name=collection_name, fields_str=fields_str, from_date=start_date, to_date=end_date, timestamp_column=timestamp_column, row_limit=None)
                                
                        batch_record_count = len(batch)
                        print(f"Record Sync: {batch_record_count}")

                        if batch_record_count>0:
                            migration_data=pg.migrate_data(mongo_df=batch, table_name=pg_table_name, unique_column=unique_column)
                                
                            if (migration_data is not True):
                                raise Exception(f"Error occured during migration of {collection_name}.")
                                
                            record_count = record_count + batch_record_count

                        # Update timestamp of ETL table
                        etl_upd_status = pg.update_last_sync_time(pg_table_name=pg_table_name, last_sync_time=start_date)

                        if (etl_upd_status is None):
                            raise Exception (f"Unable to update ETL table for {collection_name} collection.")
                    
                 
                        print(f"Inserted: {record_count}/{total_documents}")
                        self.log_info(f"Inserted: {record_count}/{total_documents}")
                        
                    print(f"{collection_name} is migrated sucessfully")
                    self.log_info(f"{collection_name} is migrated sucessfully")

                except Exception as e:
                    print(f"Error: {e}")
                    self.log_error(f"Error: {e}")
        
            return True
        
        except Exception as e:
            print(f"Error: {e}")
            self.log_error(f"Error: {e}")
            return None
        
