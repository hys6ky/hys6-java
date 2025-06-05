package hyren.serv6.commons.ocr;

import hyren.serv6.commons.utils.constant.PropertyParaValue;
import org.apache.xmlrpc.XmlRpcException;

public class PictureSeach extends XmlRpcServer {

    private static final String hexStr = "0123456789ABCDEF";

    private static final String[] binaryArray = { "0000", "0001", "0010", "0011", "0100", "0101", "0110", "0111", "1000", "1001", "1010", "1011", "1100", "1101", "1110", "1111" };

    private final static String PIC_SERVER_ADDRESS = PropertyParaValue.getString("pic_rpc_cpu", "");

    public PictureSeach() {
        super(PIC_SERVER_ADDRESS);
    }

    @Override
    public String byteToStr(byte[] bytes, String suffix) {
        String question = null;
        try {
            Object[] params = new Object[] { bytes, suffix };
            question = (String) client.execute("pHash_encode", params);
        } catch (XmlRpcException e) {
            e.printStackTrace();
        }
        return question;
    }

    public int getDistance(String upStr, String picFeature) {
        byte[] upArr = hexStr2BinArr(upStr);
        String upPicterStr = bytes2BinStr(upArr);
        byte[] sourceBytes = hexStr2BinArr(picFeature);
        String sourceStr = bytes2BinStr(sourceBytes);
        int distance;
        if (upPicterStr.length() != sourceStr.length()) {
            distance = -1;
        } else {
            distance = 0;
            for (int i = 0; i < upPicterStr.length(); i++) {
                if (upPicterStr.charAt(i) != sourceStr.charAt(i)) {
                    distance++;
                }
            }
        }
        return distance;
    }

    public byte[] hexStr2BinArr(String hexString) {
        int len = hexString.length() / 2;
        byte[] bytes = new byte[len];
        byte high;
        byte low;
        for (int i = 0; i < len; i++) {
            high = (byte) ((hexStr.indexOf(hexString.charAt(2 * i))) << 4);
            low = (byte) hexStr.indexOf(hexString.charAt(2 * i + 1));
            bytes[i] = (byte) (high | low);
        }
        return bytes;
    }

    public String bytes2BinStr(byte[] bArray) {
        StringBuilder outStr = new StringBuilder();
        int pos;
        for (byte b : bArray) {
            pos = (b & 0xF0) >> 4;
            outStr.append(binaryArray[pos]);
            pos = b & 0x0F;
            outStr.append(binaryArray[pos]);
        }
        return outStr.toString();
    }
}
