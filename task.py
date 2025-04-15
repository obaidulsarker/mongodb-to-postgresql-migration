class Task:
    def __init__(self, taskno, taskname, status, task_description, id_field_name, ts_field_name, db_name, archived_records, deleted_records):
        self.task_no = taskno
        self.task_name = taskname
        self.task_status = status
        self.task_description = task_description
        self.id_field_name = id_field_name
        self.ts_field_name = ts_field_name
        self.db_name=db_name
        self.archived_records=archived_records
        self.deleted_records=deleted_records

    def __str__(self):
        return f"TaskNo: {self.task_no}, TaskName: {self.task_name}, Status: {self.task_status}, Description: {self.task_description}, id_field_name: {self.id_field_name}, ts_field_name: {self.ts_field_name}, db_name: {self.db_name} "