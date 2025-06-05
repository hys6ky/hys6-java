package hyren.serv6.agent.job.biz.bean;

import fd.ng.core.annotation.DocBean;
import hyren.serv6.base.entity.ObjectCollectStruct;
import hyren.serv6.base.entity.ObjectHandleType;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import java.io.Serializable;
import java.util.List;

public class ObjectTableBean implements Serializable {

    @DocBean(name = "ocs_id", value = "", dataType = Long.class, required = true)
    private String ocs_id;

    @DocBean(name = "en_name", value = "", dataType = String.class, required = true)
    private String en_name;

    @DocBean(name = "hyren_name", value = "", dataType = String.class, required = true)
    private String hyren_name;

    @DocBean(name = "zh_name", value = "", dataType = String.class, required = true)
    private String zh_name;

    @DocBean(name = "collect_data_type", value = "", dataType = String.class, required = true)
    private String collect_data_type;

    @DocBean(name = "database_code", value = "", dataType = String.class, required = true)
    private String database_code;

    @DocBean(name = "agent_id", value = "", dataType = Long.class, required = true)
    private Long agent_id;

    @DocBean(name = "firstline", value = "", dataType = String.class, required = false)
    private String firstline;

    @DocBean(name = "odc_id", value = "", dataType = Long.class, required = false)
    private String odc_id;

    @DocBean(name = "updatetype", value = "", dataType = String.class, required = true)
    private String updatetype;

    @DocBean(name = "etlDate", value = "", dataType = String.class)
    private String etlDate;

    @DocBean(name = "datasource_name", value = "", dataType = String.class)
    private String datasource_name;

    @DocBean(name = "agent_name", value = "", dataType = String.class)
    private String agent_name;

    @DocBean(name = "user_id", value = "", dataType = Long.class)
    private Long user_id;

    @DocBean(name = "source_id", value = "", dataType = Long.class)
    private Long source_id;

    @DocBean(name = "object_collect_structList", value = "", dataType = List.class)
    private List<ObjectCollectStruct> object_collect_structList;

    @DocBean(name = "object_handle_typeList", value = "", dataType = List.class)
    private List<ObjectHandleType> object_handle_typeList;

    @DocBean(name = "dataStoreConfBean", value = "", dataType = DataStoreConfBean.class)
    private List<DataStoreConfBean> dataStoreConfBean;

    public String getOcs_id() {
        return ocs_id;
    }

    public void setOcs_id(String ocs_id) {
        this.ocs_id = ocs_id;
    }

    public String getEn_name() {
        return en_name;
    }

    public void setEn_name(String en_name) {
        this.en_name = en_name;
    }

    public String getZh_name() {
        return zh_name;
    }

    public void setZh_name(String zh_name) {
        this.zh_name = zh_name;
    }

    public String getCollect_data_type() {
        return collect_data_type;
    }

    public void setCollect_data_type(String collect_data_type) {
        this.collect_data_type = collect_data_type;
    }

    public String getDatabase_code() {
        return database_code;
    }

    public void setDatabase_code(String database_code) {
        this.database_code = database_code;
    }

    public Long getAgent_id() {
        return agent_id;
    }

    public void setAgent_id(Long agent_id) {
        this.agent_id = agent_id;
    }

    public String getFirstline() {
        return firstline;
    }

    public void setFirstline(String firstline) {
        this.firstline = firstline;
    }

    public String getOdc_id() {
        return odc_id;
    }

    public void setOdc_id(String odc_id) {
        this.odc_id = odc_id;
    }

    public String getUpdatetype() {
        return updatetype;
    }

    public void setUpdatetype(String updatetype) {
        this.updatetype = updatetype;
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

    public List<ObjectCollectStruct> getObject_collect_structList() {
        return object_collect_structList;
    }

    public void setObject_collect_structList(List<ObjectCollectStruct> object_collect_structList) {
        this.object_collect_structList = object_collect_structList;
    }

    public List<ObjectHandleType> getObject_handle_typeList() {
        return object_handle_typeList;
    }

    public void setObject_handle_typeList(List<ObjectHandleType> object_handle_typeList) {
        this.object_handle_typeList = object_handle_typeList;
    }

    public List<DataStoreConfBean> getDataStoreConfBean() {
        return dataStoreConfBean;
    }

    public void setDataStoreConfBean(List<DataStoreConfBean> dataStoreConfBean) {
        this.dataStoreConfBean = dataStoreConfBean;
    }

    public String getHyren_name() {
        return hyren_name;
    }

    public void setHyren_name(String hyren_name) {
        this.hyren_name = hyren_name;
    }
}
