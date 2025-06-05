package hyren.serv6.commons.utils.stream;

import java.util.Map;

public interface ConsumerBusinessProcess<T> {

    int processOrder(T t, int partitionNum, Map<String, Object> jsonStore);
}
