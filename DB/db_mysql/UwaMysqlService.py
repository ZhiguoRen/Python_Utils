# -*- coding: utf-8 -*-

from db_mysql.MysqlConnectionPool import DBControllerPool

class UwaMysqlService:
    def __init__(self, host, port, db_name, db_user_name, db_psw, max_pool_size=20,conn_at_start=True):
        #self.log = {}
        #todo:换用DBUtils连接池/换用sqlalchemy（自带连接池）
        self.dbc_pool = DBControllerPool(host=host,port=port,db_name=db_name,db_user_name=db_user_name,db_psw=db_psw,max_pool_size=max_pool_size,conn_at_start=conn_at_start)

    def find_proj_info(self, proj_id):
        try:
            table_pa_profiling_record = "pa_profiling_record"
            fields_pa_profiling_record = ["pa_project_id", "owner_id", "uploader_id", "test_index", "service_level",
                                          "create_at", "start_at", "complete_at", "update_at"]
            conditions_pa_profiling_record = ["pa_project_id"]

            table_pa_project_info = "pa_project_info"
            fields_pa_project_info = ["common_project_id", "platform", "engine", "type", "sub_type"]
            conditions_pa_project_info = ["id"]

            table_project_common_info = "project_common_info"
            fields_project_common_info = ["name"]
            conditions_project_common_info = ["id"]

            #_logger.info("find proj (%s) info"%str(proj_id))
            c_dbc = self.dbc_pool.get_dbc()
            if not c_dbc:
                return []

            from_pa_profiling_record = c_dbc.select_with_felds(table_pa_profiling_record, fields_pa_profiling_record,
                                                               conditions_pa_profiling_record, True, proj_id)
            if len(from_pa_profiling_record) <= 0:
                c_dbc = self.dbc_pool.return_dbc(c_dbc)
                return []

            from_pa_project_info = c_dbc.select_with_felds(table_pa_project_info,fields_pa_project_info,conditions_pa_project_info,True, proj_id)
            proj_common_id=from_pa_project_info[0]["common_project_id"]
            from_project_common_info = c_dbc.select_with_felds(table_project_common_info,
                                                               fields_project_common_info,
                                                               conditions_project_common_info, True, proj_common_id)
            rst = []
            for item in from_pa_profiling_record:
                merge_dict = (dict(item, **(from_pa_project_info[0])))
                merge_dict.update(from_project_common_info[0])
                rst.append(merge_dict)
            #print rst
            c_dbc=self.dbc_pool.return_dbc(c_dbc)
            return rst
        except Exception,e:
            #_logger.error(e)
            #_logger.error("except in find_proj_info")
            #_logger.error("proj_id"+str(proj_id))
            print e
            print "except in find proj_info"
            if c_dbc:
                c_dbc=self.dbc_pool.return_dbc(c_dbc)
            rst=[]
            return rst
    def stop(self):
        self.dbc_pool.close()



