package hyren.serv6.commons.enumtools;

public enum StageConstant implements EnumConstantInterface {

    UNLOADDATA(1, "unloaddata"), UPLOAD(2, "upload"), DATALOADING(3, "dataloading"), CALINCREMENT(4, "calincrement"), DATAREGISTRATION(5, "dataregistration");

    private final int code;

    private final String desc;

    StageConstant(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return "StageConstant{" + "code=" + code + ", desc='" + desc + '\'' + '}';
    }
}
