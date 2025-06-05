package hyren.serv6.commons.utils.stream;

import hyren.serv6.commons.utils.stream.bean.BusinessEnum;
import org.apache.avro.generic.GenericRecord;
import java.util.Map;

public interface RealizeBusinessProcess {

    public abstract BusinessEnum process(Map<String, Object> message);

    public abstract BusinessEnum process(GenericRecord message);

    public abstract BusinessEnum process(byte[] message);
}
