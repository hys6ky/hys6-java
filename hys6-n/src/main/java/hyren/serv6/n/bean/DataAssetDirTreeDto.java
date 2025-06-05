package hyren.serv6.n.bean;

import lombok.Data;

@Data
public class DataAssetDirTreeDto {

    private long id;

    private String name;

    private String label;

    private String code;

    private String isLeaf;

    private String status;

    private String type;
}
