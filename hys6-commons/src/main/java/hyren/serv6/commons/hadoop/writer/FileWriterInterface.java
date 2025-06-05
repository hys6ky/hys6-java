package hyren.serv6.commons.hadoop.writer;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;

@DocClass(desc = "", author = "WangZhengcheng")
public interface FileWriterInterface {

    @Method(desc = "", logicStep = "")
    @Param(name = "metaDataMap", desc = "", range = "")
    @Return(desc = "", range = "")
    String writeFiles();
}
