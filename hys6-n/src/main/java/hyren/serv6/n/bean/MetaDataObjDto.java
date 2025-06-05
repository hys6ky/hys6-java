package hyren.serv6.n.bean;

import lombok.Data;

@Data
public class MetaDataObjDto {

    private String objId;

    private String enName;

    private String chName;

    private String type;

    private String layer;

    private String isConfig;

    private long source_id;

    private String source_name;
}
