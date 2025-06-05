package hyren.serv6.h.process.args;

import lombok.Getter;
import java.util.List;

@Getter
public class HiveHandleArgs extends HandleArgs {

    private static final long serialVersionUID = 393079221171639078L;

    private String database;

    private List<String> partitionFields;

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setPartitionFields(List<String> partitionFields) {
        this.partitionFields = partitionFields;
    }
}
