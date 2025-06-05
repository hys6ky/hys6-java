package hyren.serv6.b.batchcollection.cdcCollect.req;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class FlinkCDCTable implements Serializable {

    private static final long serialVersionUID = 7403603512100754840L;

    private String table_name;

    private String topic;

    private List<KafkaInfo> kafkaInfos;

    public void addKafka(KafkaInfo info) {
        if (this.kafkaInfos == null) {
            this.kafkaInfos = new ArrayList<KafkaInfo>();
        }
        this.kafkaInfos.add(info);
    }
}
