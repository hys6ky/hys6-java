package hyren.serv6.h.process.spark.func.bean;

import lombok.Getter;

public class FunctionBean {

    @Getter
    private String name;

    @Getter
    private String className;

    private String dataType;

    public FunctionBean(String[] args) {
        this(args[0], args[1], args[2]);
    }

    public FunctionBean(String name, String className, String dataType) {
        this.name = name;
        this.className = className;
        this.dataType = dataType;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getDateType() {
        return dataType;
    }

    public void setDateType(String dateType) {
        this.dataType = dateType;
    }

    @Override
    public String toString() {
        return "Function [name=" + name + ", className=" + className + ", dataType=" + dataType + "]";
    }
}
