package hyren.serv6.base.utils.Aes;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.util.Base64;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2021-05-20 15:03")
@Slf4j
public class AesUtil {

    private static final String KEY_ALGORITHM = "AES";

    public static final String PUBLIC_KEY = "525tPh0CM6t5pa5p";

    public static final String PRIVATE_KEY = "Rko68l71rLiQBRa5";

    @Method(desc = "", logicStep = "")
    @Param(name = "data", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String encrypt(String data) {
        try {
            Validator.notBlank(data, "加密数据不能为空");
            Validator.notBlank(PUBLIC_KEY, "加密时的公钥Key值不能为空");
            Validator.notBlank(PRIVATE_KEY, "加密时的私钥Key值不能为空");
            Cipher cipher = Cipher.getInstance(RsaUtil.ALGORITHMS);
            int blockSize = cipher.getBlockSize();
            byte[] dataBytes = data.getBytes();
            int plaintextLength = dataBytes.length;
            if (plaintextLength % blockSize != 0) {
                plaintextLength = plaintextLength + (blockSize - (plaintextLength % blockSize));
            }
            byte[] plaintext = new byte[plaintextLength];
            System.arraycopy(dataBytes, 0, plaintext, 0, dataBytes.length);
            SecretKeySpec keyspec = new SecretKeySpec(PUBLIC_KEY.getBytes(), KEY_ALGORITHM);
            IvParameterSpec ivspec = new IvParameterSpec(PRIVATE_KEY.getBytes());
            cipher.init(Cipher.ENCRYPT_MODE, keyspec, ivspec);
            byte[] encrypted = cipher.doFinal(plaintext);
            return Base64.getEncoder().encodeToString(encrypted);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("数据加密失败!!!");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "data", desc = "", range = "")
    @Return(desc = "", range = "")
    public static String desEncrypt(String data) {
        try {
            Validator.notBlank(data, "解密数据不能为空");
            Validator.notBlank(PUBLIC_KEY, "解密时的公钥Key值不能为空");
            Validator.notBlank(PRIVATE_KEY, "解密时的私钥Key值不能为空");
            byte[] encrypted1 = Base64.getDecoder().decode(data);
            Cipher cipher = Cipher.getInstance(RsaUtil.ALGORITHMS);
            SecretKeySpec keySpec = new SecretKeySpec(PUBLIC_KEY.getBytes(), KEY_ALGORITHM);
            IvParameterSpec ivSpec = new IvParameterSpec(PRIVATE_KEY.getBytes());
            cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
            byte[] original = cipher.doFinal(encrypted1);
            return new String(original, Charset.defaultCharset()).trim();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("数据解析错误");
        }
    }
}
