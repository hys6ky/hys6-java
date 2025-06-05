package hyren.serv6.commons.utils.stream;

import hyren.serv6.commons.utils.stream.bean.BusinessEnum;
import org.apache.avro.generic.GenericRecord;
import java.util.Map;

public class TimeStop implements RealizeBusinessProcess {

    @Override
    public BusinessEnum process(Map<String, Object> message) {
        if (Long.parseLong(message.get("fileTime").toString()) >= KafkaConsumerRunable.endTime) {
            return BusinessEnum.stop;
        } else {
            return BusinessEnum.keep;
        }
    }

    @Override
    public BusinessEnum process(GenericRecord message) {
        if (Long.parseLong(message.get("fileTime").toString()) >= KafkaConsumerRunable.endTime) {
            return BusinessEnum.stop;
        } else {
            return BusinessEnum.keep;
        }
    }

    @Override
    public BusinessEnum process(byte[] message) {
        return null;
    }
}
