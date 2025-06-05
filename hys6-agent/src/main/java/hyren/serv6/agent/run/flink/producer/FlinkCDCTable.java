package hyren.serv6.agent.run.flink.producer;

import java.io.Serializable;
import java.util.List;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.run.flink.KafkaInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlinkCDCTable implements Serializable {

    private static final long serialVersionUID = 7403603512100754840L;

    private String table_name;

    private String topic;

    private List<KafkaInfo> kafkaInfos;

    public void verify() {
        if (StringUtil.isEmpty(table_name)) {
            throw new RuntimeException("数据表不可为空");
        }
        if (StringUtil.isEmpty(topic)) {
            throw new RuntimeException("主题不可为空");
        }
        if (kafkaInfos == null || kafkaInfos.size() <= 0) {
            throw new RuntimeException("目标kafka 不可为空");
        }
    }
}
