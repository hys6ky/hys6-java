package hyren.serv6.commons.utils.datastorage;

import fd.ng.core.annotation.DocClass;
import hyren.serv6.commons.utils.datastorage.dcl.LengthMapping;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "20210-12-20 15:38")
public class QueryLengthMapping {

    public static final String CONF_FILE_NAME = "lengthMapping.conf";

    public static Map<String, List<Map<String, Object>>> getLengthMapping() {
        return new LengthMapping().yamlDataFormat(null);
    }
}
