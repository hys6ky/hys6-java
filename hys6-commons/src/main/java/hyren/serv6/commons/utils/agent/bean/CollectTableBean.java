package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.conf.Dbtype;
import hyren.serv6.base.entity.ColumnMerge;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.entity.TbcolSrctgtMap;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/29 14:26")
public class CollectTableBean implements Serializable {

    @DocBean(name = "database_id", value = "", dataType = Long.class)
    private String database_id;

    @DocBean(name = "table_id", value = "", dataType = Long.class)
    private String table_id;

    @DocBean(name = "table_name", value = "", dataType = String.class)
    private String table_name;

    @DocBean(name = "table_ch_name", value = "", dataType = String.class)
    private String table_ch_name;

    @DocBean(name = "table_count", value = "", dataType = String.class, required = false)
    private String table_count;

    @DocBean(name = "source_tableid", value = "", dataType = String.class, required = false)
    private String source_tableid;

    @DocBean(name = "valid_s_date", value = "", dataType = String.class)
    private String valid_s_date;

    @DocBean(name = "valid_e_date", value = "", dataType = String.class)
    private String valid_e_date;

    @DocBean(name = "sql", value = "", dataType = String.class, required = false)
    private String sql;

    @DocBean(name = "remark", value = "", dataType = String.class, required = false)
    private String remark;

    @DocBean(name = "is_user_defined", value = "", dataType = String.class)
    private String is_user_defined;

    @DocBean(name = "is_md5", value = "", dataType = String.class)
    private String is_md5;

    @DocBean(name = "is_register", value = "", dataType = String.class)
    private String is_register;

    @DocBean(name = "is_parallel", value = "", dataType = String.class)
    private String is_parallel;

    @DocBean(name = "page_sql", value = "", dataType = String.class, required = false)
    private String page_sql;

    @DocBean(name = "pageparallels", value = "", dataType = Integer.class, required = false)
    private Integer pageparallels;

    @DocBean(name = "dataincrement", value = "", dataType = Integer.class, required = false)
    private Integer dataincrement;

    @DocBean(name = "rec_num_date", value = "", dataType = String.class)
    private String rec_num_date;

    @DocBean(name = "data_extraction_def_list", value = "", dataType = List.class)
    private List<DataExtractionDef> data_extraction_def_list;

    @DocBean(name = "collectTableColumnBeanList", value = "", dataType = String.class, required = false)
    private List<CollectTableColumnBean> collectTableColumnBeanList;

    @DocBean(name = "column_merge_list", value = "", dataType = ColumnMerge.class, required = false)
    private List<ColumnMerge> column_merge_list;

    @DocBean(name = "storage_type", value = "", dataType = String.class)
    private String storage_type;

    @DocBean(name = "storage_time", value = "", dataType = Long.class)
    private Long storage_time;

    @DocBean(name = "is_zipper", value = "", dataType = String.class)
    private String is_zipper;

    @DocBean(name = "dataStoreConfBean", value = "", dataType = DataStoreConfBean.class)
    private List<DataStoreConfBean> dataStoreConfBean;

    @DocBean(name = "storage_table_name", value = "", dataType = String.class)
    private String storage_table_name;

    @DocBean(name = "etlDate", value = "", dataType = String.class)
    private String etlDate;

    @DocBean(name = "datasource_name", value = "", dataType = String.class)
    private String datasource_name;

    @DocBean(name = "agent_name", value = "", dataType = String.class)
    private String agent_name;

    @DocBean(name = "agent_id", value = "", dataType = Long.class)
    private Long agent_id;

    @DocBean(name = "user_id", value = "", dataType = Long.class)
    private Long user_id;

    @DocBean(name = "source_id", value = "", dataType = Long.class)
    private Long source_id;

    @DocBean(name = "sqlParam", value = "", dataType = String.class)
    private String sqlParam;

    @DocBean(name = "unload_type", value = "", dataType = String.class)
    private String unload_type;

    @DocBean(name = "is_customize_sql", value = "", dataType = String.class)
    private String is_customize_sql;

    @DocBean(name = "storage_date", value = "", dataType = String.class)
    private String storage_date;

    @DocBean(name = "selectFileFormat", value = "", dataType = String.class)
    private String selectFileFormat;

    @DocBean(name = "interval_time", value = "", dataType = Integer.class, required = false)
    private Integer interval_time;

    @DocBean(name = "over_date", value = "", dataType = String.class, required = false)
    private String over_date;

    @DocBean(name = "db_type", value = "", dataType = String.class, required = false)
    private Dbtype db_type;

    @DocBean(name = "tbColTarTypeMaps", value = "", dataType = TbcolSrctgtMap.class, required = true)
    private List<TbColTarTypeMapBean> tbColTarTypeMaps;

    public Dbtype getDb_type() {
        return db_type;
    }

    public void setDb_type(Dbtype db_type) {
        this.db_type = db_type;
    }

    public List<ColumnMerge> getColumn_merge_list() {
        return column_merge_list;
    }

    public void setColumn_merge_list(List<ColumnMerge> column_merge_list) {
        this.column_merge_list = column_merge_list;
    }

    public String getTable_id() {
        return table_id;
    }

    public void setTable_id(String table_id) {
        this.table_id = table_id;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getTable_ch_name() {
        return table_ch_name;
    }

    public void setTable_ch_name(String table_ch_name) {
        this.table_ch_name = table_ch_name;
    }

    public String getTable_count() {
        return table_count;
    }

    public void setTable_count(String table_count) {
        this.table_count = table_count;
    }

    public String getRec_num_date() {
        return rec_num_date;
    }

    public void setRec_num_date(String rec_num_date) {
        this.rec_num_date = rec_num_date;
    }

    public String getSource_tableid() {
        return source_tableid;
    }

    public void setSource_tableid(String source_tableid) {
        this.source_tableid = source_tableid;
    }

    public String getValid_s_date() {
        return valid_s_date;
    }

    public void setValid_s_date(String valid_s_date) {
        this.valid_s_date = valid_s_date;
    }

    public String getValid_e_date() {
        return valid_e_date;
    }

    public void setValid_e_date(String valid_e_date) {
        this.valid_e_date = valid_e_date;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String getIs_user_defined() {
        return is_user_defined;
    }

    public void setIs_user_defined(String is_user_defined) {
        this.is_user_defined = is_user_defined;
    }

    public String getIs_md5() {
        return is_md5;
    }

    public void setIs_md5(String is_md5) {
        this.is_md5 = is_md5;
    }

    public String getIs_register() {
        return is_register;
    }

    public void setIs_register(String is_register) {
        this.is_register = is_register;
    }

    public String getIs_parallel() {
        return is_parallel;
    }

    public void setIs_parallel(String is_parallel) {
        this.is_parallel = is_parallel;
    }

    public String getPage_sql() {
        return page_sql;
    }

    public void setPage_sql(String page_sql) {
        this.page_sql = page_sql;
    }

    public List<DataExtractionDef> getData_extraction_def_list() {
        return data_extraction_def_list;
    }

    public void setData_extraction_def_list(List<DataExtractionDef> data_extraction_def_list) {
        this.data_extraction_def_list = data_extraction_def_list;
    }

    public List<CollectTableColumnBean> getCollectTableColumnBeanList() {
        return collectTableColumnBeanList;
    }

    public void setCollectTableColumnBeanList(List<CollectTableColumnBean> collectTableColumnBeanList) {
        this.collectTableColumnBeanList = collectTableColumnBeanList;
    }

    public String getStorage_type() {
        return storage_type;
    }

    public void setStorage_type(String storage_type) {
        this.storage_type = storage_type;
    }

    public Long getStorage_time() {
        return storage_time;
    }

    public void setStorage_time(Long storage_time) {
        this.storage_time = storage_time;
    }

    public String getIs_zipper() {
        return is_zipper;
    }

    public void setIs_zipper(String is_zipper) {
        this.is_zipper = is_zipper;
    }

    public List<DataStoreConfBean> getDataStoreConfBean() {
        return dataStoreConfBean;
    }

    public void setDataStoreConfBean(List<DataStoreConfBean> dataStoreConfBean) {
        this.dataStoreConfBean = dataStoreConfBean;
    }

    public String getStorage_table_name() {
        return storage_table_name;
    }

    public void setStorage_table_name(String storage_table_name) {
        this.storage_table_name = storage_table_name;
    }

    public String getDatabase_id() {
        return database_id;
    }

    public void setDatabase_id(String database_id) {
        this.database_id = database_id;
    }

    public String getEtlDate() {
        return etlDate;
    }

    public void setEtlDate(String etlDate) {
        this.etlDate = etlDate;
    }

    public String getDatasource_name() {
        return datasource_name;
    }

    public void setDatasource_name(String datasource_name) {
        this.datasource_name = datasource_name;
    }

    public String getAgent_name() {
        return agent_name;
    }

    public void setAgent_name(String agent_name) {
        this.agent_name = agent_name;
    }

    public Long getAgent_id() {
        return agent_id;
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public Long getSource_id() {
        return source_id;
    }

    public void setSource_id(Long source_id) {
        this.source_id = source_id;
    }

    public Integer getPageparallels() {
        return pageparallels;
    }

    public void setPageparallels(Integer pageparallels) {
        this.pageparallels = pageparallels;
    }

    public Integer getDataincrement() {
        return dataincrement;
    }

    public void setDataincrement(Integer dataincrement) {
        this.dataincrement = dataincrement;
    }

    public String getSqlParam() {
        return sqlParam;
    }

    public void setSqlParam(String sqlParam) {
        this.sqlParam = sqlParam;
    }

    public String getUnload_type() {
        return unload_type;
    }

    public void setUnload_type(String unload_type) {
        this.unload_type = unload_type;
    }

    public String getIs_customize_sql() {
        return is_customize_sql;
    }

    public void setIs_customize_sql(String is_customize_sql) {
        this.is_customize_sql = is_customize_sql;
    }

    public String getStorage_date() {
        return storage_date;
    }

    public void setStorage_date(String storage_date) {
        this.storage_date = storage_date;
    }

    public String getSelectFileFormat() {
        return selectFileFormat;
    }

    public void setSelectFileFormat(String selectFileFormat) {
        this.selectFileFormat = selectFileFormat;
    }

    public Integer getInterval_time() {
        return interval_time;
    }

    public void setInterval_time(Integer interval_time) {
        this.interval_time = interval_time;
    }

    public String getOver_date() {
        return over_date;
    }

    public void setOver_date(String over_date) {
        this.over_date = over_date;
    }

    public List<TbColTarTypeMapBean> getTbColTarTypeMaps() {
        return tbColTarTypeMaps;
    }

    public void setTbColTarTypeMaps(List<TbColTarTypeMapBean> tbColTarTypeMaps) {
        this.tbColTarTypeMaps = tbColTarTypeMaps;
    }
}
