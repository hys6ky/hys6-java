package hyren.serv6.k.utils.easyexcel;

public enum ValidRuleEnum {

    NOT_NULL("^\\S+$", "不能为空"), NUMBER("^-?\\d+$", "只能是整数"), FLOAT("^-?\\d+\\.\\d+$", "只能是浮点数数"), NUMERIC("^-?\\d+(\\.\\d+)?$", "只能是数值型");

    private String regular;

    private String msg;

    ValidRuleEnum(String regular, String msg) {
        this.regular = regular;
        this.msg = msg;
    }

    public String getRegular() {
        return regular;
    }

    public void setRegular(String regular) {
        this.regular = regular;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
