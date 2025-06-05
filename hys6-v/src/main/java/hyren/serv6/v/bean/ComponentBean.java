package hyren.serv6.v.bean;

import fd.ng.core.annotation.DocBean;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.entity.anno.Table;
import hyren.serv6.base.entity.fdentity.ProEntity;

@DocClass(desc = "", author = "dhw", createdate = "2020/9/2 10:03")
@Table(tableName = "component_bean")
public class ComponentBean extends ProEntity {

    @DocBean(name = "showNum", value = "", dataType = Integer.class)
    private Integer showNum;

    @DocBean(name = "condition_sql", value = "", dataType = String.class)
    private String condition_sql;

    @DocBean(name = "fetch_name", value = "", dataType = String.class)
    private String fetch_name;

    @DocBean(name = "data_source", value = "", dataType = String.class)
    private String data_source;

    @DocBean(name = "x_columns", value = "", dataType = String[].class)
    private String[] x_columns;

    @DocBean(name = "y_columns", value = "", dataType = String[].class)
    private String[] y_columns;

    public Integer getShowNum() {
        return showNum;
    }

    public void setShowNum(Integer showNum) {
        this.showNum = showNum;
    }

    public String getCondition_sql() {
        return condition_sql;
    }

    public void setCondition_sql(String condition_sql) {
        this.condition_sql = condition_sql;
    }

    public String getFetch_name() {
        return fetch_name;
    }

    public void setFetch_name(String fetch_name) {
        this.fetch_name = fetch_name;
    }

    public String getData_source() {
        return data_source;
    }

    public void setData_source(String data_source) {
        this.data_source = data_source;
    }

    public String[] getX_columns() {
        return x_columns;
    }

    public void setX_columns(String[] x_columns) {
        this.x_columns = x_columns;
    }

    public String[] getY_columns() {
        return y_columns;
    }

    public void setY_columns(String[] y_columns) {
        this.y_columns = y_columns;
    }
}
