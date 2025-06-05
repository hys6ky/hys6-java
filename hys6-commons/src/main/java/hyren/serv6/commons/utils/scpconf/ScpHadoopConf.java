package hyren.serv6.commons.utils.scpconf;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DataStoreLayerAttr;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.jsch.AgentDeploy;
import hyren.serv6.base.utils.jsch.SSHOperate;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-02-17 13:31")
@Slf4j
public class ScpHadoopConf {

    private static final String STORE_CONFIG_PATH = "storeConfigPath";

    public static void scpConfToAgent(String targetPath, SSHOperate sshOperate) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            List<Map<String, Object>> list = SqlOperator.queryList(db, "select t1.dsl_name,t2.storage_property_key,t2.storage_property_val from " + DataStoreLayer.TableName + " t1 " + "join " + DataStoreLayerAttr.TableName + " t2 on t1.dsl_id = t2.dsl_id where t2.is_file = ?", IsFlag.Shi.getCode());
            String targetDir = targetPath + AgentDeploy.SEPARATOR + STORE_CONFIG_PATH + AgentDeploy.SEPARATOR;
            list.forEach(item -> {
                String dsl_name = ((String) item.get("dsl_name")).trim();
                String orginalFileName = ((String) item.get("storage_property_key")).trim();
                String localFilePath = ((String) item.get("storage_property_val")).trim();
                String targetMachineConf = targetDir + dsl_name;
                try {
                    if (!new File(localFilePath).exists()) {
                        log.info("本地文件: " + localFilePath + " 不存在,跳过!!!");
                    } else {
                        sshOperate.execCommandBySSHNoRs("mkdir -p " + targetMachineConf);
                        log.info("开始传输集群XML: " + localFilePath + " 到目录 :" + targetMachineConf);
                        sshOperate.channelSftp.put(localFilePath, targetMachineConf);
                    }
                } catch (SftpException e) {
                    log.error(e.getMessage(), e);
                    throw new BusinessException("创建远程目录  " + targetMachineConf + "  失败!!!");
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    throw new BusinessException(e.getMessage());
                }
            });
        }
    }

    private static boolean isExistDir(String path, ChannelSftp sftp) {
        boolean isExist = false;
        try {
            SftpATTRS sftpATTRS = sftp.lstat(path);
            isExist = true;
            return sftpATTRS.isDir();
        } catch (Exception e) {
            if (e.getMessage().toLowerCase().equals("no such file")) {
                isExist = false;
            }
        }
        return isExist;
    }
}
