package hyren.serv6.stream.agent.util;

public abstract class TranBase {

    public abstract String transact(String bit_head, String head_msg, String component, String job_key, String msg) throws ClassNotFoundException, InstantiationException, IllegalAccessException, Exception;
}
