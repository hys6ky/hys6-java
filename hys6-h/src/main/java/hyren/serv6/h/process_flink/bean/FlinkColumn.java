package hyren.serv6.h.process_flink.bean;

import org.apache.flink.table.types.DataType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkColumn {

    private String name;

    private DataType type;

    private boolean isPrimary;
}
