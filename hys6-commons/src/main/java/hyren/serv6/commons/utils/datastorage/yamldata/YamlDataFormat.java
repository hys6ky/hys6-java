package hyren.serv6.commons.utils.datastorage.yamldata;

import fd.ng.core.annotation.DocClass;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-01-15 10:04")
public interface YamlDataFormat {

    Map<String, List<Map<String, Object>>> yamlDataFormat(String dbBatch_row);
}
