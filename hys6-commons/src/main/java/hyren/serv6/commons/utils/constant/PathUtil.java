package hyren.serv6.commons.utils.constant;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.exception.BusinessException;

@DocClass(desc = "", author = "BY-HLL", createdate = "2019/10/17 0017")
public class PathUtil {

    public static final String ISL = DataSourceType.ISL.getCode();

    public static final String DCL = DataSourceType.DCL.getCode();

    public static final String DPL = DataSourceType.DPL.getCode();

    public static final String DML = DataSourceType.DML.getCode();

    public static final String SFL = DataSourceType.SFL.getCode();

    public static final String AML = DataSourceType.AML.getCode();

    public static final String DQC = DataSourceType.DQC.getCode();

    public static final String UDL = DataSourceType.UDL.getCode();

    @Method(desc = "", logicStep = "")
    @Param(name = "localPath", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String convertLocalPathToHDFSPath(String localPath) {
        localPath = FileNameUtils.normalize(localPath).replace("\\", "/");
        String HDFSPath;
        try {
            HDFSPath = localPath.substring(localPath.lastIndexOf(CommonVariables.PREFIX));
        } catch (StringIndexOutOfBoundsException e) {
            throw new BusinessException("本地文件路径转HDFS路径失败! localPath=" + localPath);
        }
        return HDFSPath;
    }
}
