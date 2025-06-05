package hyren.serv6.commons.utils.agent.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import java.io.Serializable;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/11/29 14:52")
public class CollectTableColumnBean implements Serializable {

    @DocBean(name = "column_id", value = "", dataType = Long.class, required = true)
    private Long column_id;

    @DocBean(name = "is_primary_key", value = "", dataType = String.class, required = true)
    private String is_primary_key;

    @DocBean(name = "column_name", value = "", dataType = String.class, required = true)
    private String column_name;

    @DocBean(name = "column_ch_name", value = "", dataType = String.class, required = false)
    private String column_ch_name;

    @DocBean(name = "valid_s_date", value = "", dataType = String.class, required = true)
    private String valid_s_date;

    @DocBean(name = "valid_e_date", value = "", dataType = String.class, required = true)
    private String valid_e_date;

    @DocBean(name = "is_get", value = "", dataType = String.class, required = false)
    private String is_get;

    @DocBean(name = "column_type", value = "", dataType = String.class, required = false)
    private String column_type;

    @DocBean(name = "column_tar_type", value = "", dataType = String.class, required = false)
    private String column_tar_type;

    @DocBean(name = "tc_remark", value = "", dataType = String.class, required = false)
    private String tc_remark;

    public String getColumn_tar_type() {
        return column_tar_type;
    }

    public void setColumn_tar_type(String column_tar_type) {
        this.column_tar_type = column_tar_type;
    }

    @DocBean(name = "is_alive", value = "", dataType = String.class, required = true)
    private String is_alive;

    @DocBean(name = "is_new", value = "", dataType = String.class, required = true)
    private String is_new;

    @DocBean(name = "tc_or", value = "", dataType = String.class, required = false)
    private String tc_or;

    @DocBean(name = "columnCleanBeanList", value = "", dataType = List.class, required = false)
    private List<ColumnCleanBean> columnCleanBeanList;

    @DocBean(name = "is_zipper_field", value = "", dataType = String.class, required = true)
    private String is_zipper_field;

    public Long getColumn_id() {
        return column_id;
    }

    public void setColumn_id(Long column_id) {
        this.column_id = column_id;
    }

    public String getIs_primary_key() {
        return is_primary_key;
    }

    public void setIs_primary_key(String is_primary_key) {
        this.is_primary_key = is_primary_key;
    }

    public String getColumn_name() {
        return column_name;
    }

    public void setColumn_name(String column_name) {
        this.column_name = column_name;
    }

    public String getColumn_ch_name() {
        return column_ch_name;
    }

    public void setColumn_ch_name(String column_ch_name) {
        this.column_ch_name = column_ch_name;
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

    public String getIs_get() {
        return is_get;
    }

    public void setIs_get(String is_get) {
        this.is_get = is_get;
    }

    public String getColumn_type() {
        return column_type;
    }

    public void setColumn_type(String column_type) {
        this.column_type = column_type;
    }

    public String getTc_remark() {
        return tc_remark;
    }

    public void setTc_remark(String tc_remark) {
        this.tc_remark = tc_remark;
    }

    public String getIs_alive() {
        return is_alive;
    }

    public void setIs_alive(String is_alive) {
        this.is_alive = is_alive;
    }

    public String getIs_new() {
        return is_new;
    }

    public void setIs_new(String is_new) {
        this.is_new = is_new;
    }

    public String getTc_or() {
        return tc_or;
    }

    public void setTc_or(String tc_or) {
        this.tc_or = tc_or;
    }

    public List<ColumnCleanBean> getColumnCleanBeanList() {
        return columnCleanBeanList;
    }

    public void setColumnCleanBeanList(List<ColumnCleanBean> columnCleanBeanList) {
        this.columnCleanBeanList = columnCleanBeanList;
    }

    public String getIs_zipper_field() {
        return is_zipper_field;
    }

    public void setIs_zipper_field(String is_zipper_field) {
        this.is_zipper_field = is_zipper_field;
    }
}
