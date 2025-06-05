package hyren.serv6.control.task.helper;

import com.esms.MessageData;
import com.esms.PostMsg;
import com.esms.common.entity.Account;
import com.esms.common.entity.GsmsResponse;
import com.esms.common.entity.MTPack;
import hyren.serv6.control.constans.ControlConfigure;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
public class NotifyMessageHelper {

    private final String smsAccountName;

    private final String smsAccountPasswd;

    private final String cmHostIp;

    private final int cmHostPort;

    private final String wsHostIp;

    private final int wsHostPort;

    private final String bizType;

    private final String phoneNumber;

    private static final NotifyMessageHelper INSTANCE = new NotifyMessageHelper();

    private NotifyMessageHelper() {
        this.smsAccountName = ControlConfigure.NotifyConfig.smsAccountName;
        this.smsAccountPasswd = ControlConfigure.NotifyConfig.smsAccountPasswd;
        this.cmHostIp = ControlConfigure.NotifyConfig.cmHostIp;
        this.cmHostPort = ControlConfigure.NotifyConfig.cmHostPort;
        this.wsHostIp = ControlConfigure.NotifyConfig.wsHostIp;
        this.wsHostPort = ControlConfigure.NotifyConfig.wsHostPort;
        this.bizType = ControlConfigure.NotifyConfig.bizType;
        this.phoneNumber = ControlConfigure.NotifyConfig.phoneNumber;
    }

    public static NotifyMessageHelper getInstance() {
        return INSTANCE;
    }

    public void sendMsg(String message) {
        try {
            Account ac = new Account(smsAccountName, smsAccountPasswd);
            PostMsg pm = new PostMsg();
            pm.getCmHost().setHost(cmHostIp, cmHostPort);
            pm.getWsHost().setHost(wsHostIp, wsHostPort);
            MTPack pack = new MTPack();
            pack.setBatchID(UUID.randomUUID());
            pack.setBatchName(smsAccountName + "-" + System.currentTimeMillis());
            pack.setMsgType(MTPack.MsgType.SMS);
            pack.setSendType(MTPack.SendType.MASS);
            pack.setBizType(Integer.parseInt(bizType));
            pack.setDistinctFlag(false);
            List<MessageData> msgs = new ArrayList<>();
            String[] phoneNumberArray = phoneNumber.split(",");
            for (String s : phoneNumberArray) {
                msgs.add(new MessageData(s, message));
            }
            pack.setMsgs(msgs);
            GsmsResponse resp;
            resp = pm.post(ac, pack);
            log.info(resp.getMessage());
            log.info("您的UUID为：" + resp.getUuid());
            log.info("系统返回值为：" + resp.getResult());
            log.info(msgs.toString());
        } catch (Exception e) {
            log.error("Exception:" + e.getMessage());
        }
    }
}
