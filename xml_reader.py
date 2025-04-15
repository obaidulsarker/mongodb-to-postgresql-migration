import xml.etree.ElementTree as ET
from logger import *
from setting import get_variables
from task import *


class Index(Logger):
    def __init__(self, logfile, database_name, collection_no, collection_name, fields_list, id_field_name, ts_field_name, collection_status):
        super().__init__(logfile)
        self.database_name = database_name
        self.collection_no = collection_no
        self.collection_name = collection_name
        self.id_field_name = id_field_name
        self.ts_field_name = ts_field_name
        self.fields_list=fields_list
        self.collection_status=collection_status

    def __str__(self):
        return f"DB Name:{self.database_name}, Collection No: {self.collection_no}, Collection Name: {self.collection_name}, Id Field Name: {self.id_field_name}, Timestamp Field Name: {self.ts_field_name}, Fields: {self.fields_list}, collection status: {self.collection_status}"
    
class XmlReader(Logger):
    def __init__(self, logfile):
        super().__init__(logfile)
        self.operation_log = logfile
        self.indexes_xml_file_path = get_variables().INDEXES_XML_FILE_PATH
        self.total_collection=0

    # Read collection from xml file and load into task LIST and return a list
    def get_collection_list(self):
        try:
            xml_file=self.indexes_xml_file_path

            # Parse the XML file
            tree = ET.parse(xml_file)
            root = tree.getroot()
            
            # Create a list to store Task objects
            collection_list = []
            collection_no=0
            # Iterate through the queries and execute them
            for collections_element in root.findall('collections'):
                db_name = collections_element.get('db_name')

                for query in collections_element.findall('collection'):
                    #collection_no = query.get("collection_no")
                    collection_no = collection_no + 1
                    collection_name = query.get("collection_name")
                    id_field_name = query.get("id_field_name")
                    ts_field_name = query.get("ts_field_name")
                    collection_status = query.get("collection_status")
                    fields_list = query.find("fields").text.strip()

                    index_obj = Index(logfile=self.log_file, database_name=db_name, collection_no=collection_no,collection_name=collection_name,id_field_name=id_field_name,ts_field_name=ts_field_name,fields_list=fields_list, collection_status= collection_status)
                    collection_list.extend([index_obj])
                    #print (task)
                    self.total_collection = self.total_collection +1
                
            return collection_list
        
        except Exception as e:
            self.log_error(f"Exception: {str(e)}")
            print (f"Exception: {str(e)}")
            return None
    