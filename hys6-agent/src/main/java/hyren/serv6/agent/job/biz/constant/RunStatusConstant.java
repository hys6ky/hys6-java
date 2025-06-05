package hyren.serv6.agent.job.biz.constant;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.enumtools.EnumConstantInterface;

public enum RunStatusConstant implements EnumConstantInterface {

    WAITING(1, "waiting"),
    RUNNING(2, "running"),
    COMPLETE(3, "compele"),
    SUCCEED(4, "succeed"),
    FAILED(0, "failed"),
    TERMINATED(-1, "terminated"),
    PAUSE(5, "pause");

    private final int code;

    private final String message;

    RunStatusConstant(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static String ofMessageByCode(int code) {
        for (RunStatusConstant typeCode : RunStatusConstant.values()) {
            if (typeCode.getCode() == code) {
                return typeCode.message;
            }
        }
        throw new AppSystemException("根据" + code + "没有找到对应的代码项");
    }

    @Override
    public String toString() {
        return message;
    }
}
