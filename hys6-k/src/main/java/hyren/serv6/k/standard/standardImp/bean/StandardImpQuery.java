package hyren.serv6.k.standard.standardImp.bean;

import fd.ng.db.jdbc.DefaultPageImpl;
import lombok.Data;

@Data
public class StandardImpQuery extends DefaultPageImpl {

    private Long source_id;

    private String scheamName;

    private String retrieval;

    private Long codeTypeId;
}
