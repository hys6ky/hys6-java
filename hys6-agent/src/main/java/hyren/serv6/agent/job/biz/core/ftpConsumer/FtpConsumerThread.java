package hyren.serv6.agent.job.biz.core.ftpConsumer;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.base.entity.FtpTransfered;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class FtpConsumerThread extends Thread {

    public static ConcurrentMap<String, ArrayBlockingQueue<String>> queueMap = new ConcurrentHashMap<>();

    private final String ftpId;

    private static final String addSql = "INSERT " + "INTO " + FtpTransfered.TableName + "    (" + "        ftp_transfered_id," + "        ftp_id," + "        transfered_name," + "        file_path," + "        ftp_filemd5," + "        ftp_date," + "        ftp_time" + "    ) " + "    VALUES " + "    ( ?, ?, ?, ?, ?, ?, ?)";

    public FtpConsumerThread(String ftpId) {
        this.ftpId = ftpId;
    }

    @Override
    public void run() {
        log.info("开始FtpConsumerThread程序...");
        int count = 0;
        List<Object[]> addParamsPool = new ArrayList<>();
        while (true) {
            try {
                String queueMeta = queueMap.get(ftpId).take();
                Map<String, Object> queueJb = JsonUtil.toObject(queueMeta, new TypeReference<Map<String, Object>>() {
                });
                Object end = queueJb.get("end");
                if (null != end && !Boolean.parseBoolean(end.toString())) {
                    Object[] objStrs = new Object[7];
                    objStrs[0] = UUID.randomUUID().toString();
                    objStrs[1] = Long.parseLong(ftpId);
                    objStrs[2] = queueJb.get("fileName");
                    objStrs[3] = queueJb.get("absolutePath");
                    objStrs[4] = queueJb.get("md5");
                    objStrs[5] = queueJb.get("ftpDate");
                    objStrs[6] = queueJb.get("ftpTime");
                    addParamsPool.add(objStrs);
                    count++;
                    if (count > 5000) {
                        CommunicationUtil.batchAddFtpTransfer(addParamsPool, addSql, ftpId);
                        addParamsPool.clear();
                        count = 0;
                    }
                } else {
                    if (addParamsPool.size() > 0) {
                        CommunicationUtil.batchAddFtpTransfer(addParamsPool, addSql, ftpId);
                        addParamsPool.clear();
                    }
                    break;
                }
            } catch (Exception e) {
                log.error("ftp采集消费线程记录到数据库异常", e);
            }
        }
    }
}
