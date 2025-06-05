package hyren.serv6.base.utils.Aes;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-05-20 15:20")
public class RsaUtil {

    public static final String ALGORITHMS = "AES/CBC/NoPadding";

    public static final String PUBLIC_KEY = "publicKey";

    public static final String PRIVATE_KEY = "privateKey";

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, String> getPrivateKey() {
        Map<String, String> publicMap = new HashMap<>();
        publicMap.put("privateKey", PRIVATE_KEY);
        publicMap.put("privateKeyData", getKey());
        return publicMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static Map<String, String> getPublicKey() {
        Map<String, String> publicMap = new HashMap<>();
        publicMap.put("publicKey", PUBLIC_KEY);
        publicMap.put("publicKeyData", getKey());
        return publicMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public static String getKey() {
        StringBuilder uid = new StringBuilder();
        Random rd = new SecureRandom();
        for (int i = 0; i < 16; i++) {
            int type = rd.nextInt(3);
            switch(type) {
                case 0:
                    uid.append(rd.nextInt(10));
                    break;
                case 1:
                    uid.append((char) (rd.nextInt(25) + 65));
                    break;
                case 2:
                    uid.append((char) (rd.nextInt(25) + 97));
                    break;
                default:
                    break;
            }
        }
        return uid.toString();
    }
}
