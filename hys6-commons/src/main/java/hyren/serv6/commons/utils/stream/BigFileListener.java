package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.commond.ConsumerSelector;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BigFileListener {

    private static final Logger logger = LogManager.getLogger();

    public static volatile TreeMap<Integer, String> mapFile = new TreeMap<>(Integer::compareTo);

    public static volatile ConcurrentMap<Long, BufferedOutputStream> mapBos = new ConcurrentHashMap<>();

    public static volatile TreeMap<Integer, String> mapMerge = new TreeMap<>(Integer::compareTo);

    private BufferedOutputStream bos;

    private static BufferedInputStream bis;

    private static volatile BufferedOutputStream bosSplit;

    public static volatile boolean sign = false;

    public static volatile boolean signEnd = false;

    public long waitTime = 0L;

    public static volatile int i = 0;

    private int signNum = 0;

    public volatile static String[] message;

    private int messageSize;

    private volatile byte[] bytes;

    public static volatile ReentrantLock lock = new ReentrantLock();

    public static volatile Condition condition = lock.newCondition();

    public BigFileListener(Map<String, Object> jsonsdm, String partitionType) {
        if (partitionType.equals("1")) {
            Map<String, Object> jsonStore = JsonUtil.toObject(JsonUtil.toJson(jsonsdm.get("params")), new TypeReference<Map<String, Object>>() {
            });
            String sdm_cons_des = jsonsdm.get("sdm_conf_describe").toString();
            if (sdm_cons_des.equals("14")) {
                this.waitTime = Long.parseLong(jsonStore.get("time_interval").toString());
                this.messageSize = Integer.parseInt(jsonStore.get("messageSize").toString());
                this.bytes = new byte[messageSize];
                String filePath = jsonStore.get("filePath").toString();
                String fileName = jsonStore.get("fileName").toString();
                String pathName = filePath + File.separator + fileName;
                try {
                    this.bos = new BufferedOutputStream(new FileOutputStream(pathName));
                } catch (FileNotFoundException e1) {
                    logger.error("FileNotFoundException 文件:" + pathName);
                }
                new Thread(() -> {
                    lock.lock();
                    while (true) {
                        CloseableHttpClient httpClient = null;
                        try {
                            condition.await(1000, TimeUnit.MILLISECONDS);
                            if (i > 0) {
                                if (sign || signNum == i && signEnd && i > 0) {
                                    sign = true;
                                    condition.await(waitTime, TimeUnit.MILLISECONDS);
                                    if (sign) {
                                        if (!mapFile.isEmpty()) {
                                            StringBuilder sb = new StringBuilder();
                                            for (int key : mapFile.keySet()) {
                                                sb.append(key).append(",");
                                            }
                                            sb.deleteCharAt(sb.length() - 1);
                                            httpClient = HttpClients.createDefault();
                                            String head = message[5];
                                            HttpPost httpPost = getHttpPost(message[8]);
                                            head = head.substring(0, 7) + "9004" + head.substring(11) + "1";
                                            logger.info("需要重新发送的位置：" + sb);
                                            senMsg(httpClient, httpPost, head + message[6] + "#" + sb.toString());
                                        } else {
                                            for (BufferedOutputStream bos : mapBos.values()) {
                                                IOUtils.closeQuietly(bos);
                                            }
                                            logger.info("开始合并文件：" + System.currentTimeMillis());
                                            int len;
                                            for (String path : mapMerge.values()) {
                                                bis = new BufferedInputStream(new FileInputStream(path));
                                                long countSize = new File(path).length();
                                                long fileSize = messageSize;
                                                int num;
                                                if (countSize % fileSize == 0) {
                                                    num = (int) (countSize / fileSize);
                                                } else {
                                                    num = (int) (countSize / fileSize) + 1;
                                                }
                                                for (int i = 0; i < num; i++) {
                                                    while ((len = bis.read(bytes)) != -1) {
                                                        bos.write(bytes, 0, len);
                                                    }
                                                }
                                                IOUtils.closeQuietly(bis);
                                                File file = new File(path);
                                                if (file.exists()) {
                                                    if (!file.delete()) {
                                                        throw new BusinessException("删除文件失败! " + file.getAbsolutePath());
                                                    }
                                                }
                                            }
                                            stop();
                                            logger.info("endTime: " + System.currentTimeMillis());
                                            lock.unlock();
                                            ConsumerSelector.flag = false;
                                            break;
                                        }
                                    }
                                }
                                signNum = i;
                            }
                        } catch (InterruptedException | IOException e) {
                            logger.info(logger, e);
                            IOUtils.closeQuietly(bosSplit);
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                        } finally {
                            if (httpClient != null) {
                                try {
                                    httpClient.close();
                                } catch (IOException e) {
                                    logger.info(logger, e);
                                }
                            }
                        }
                    }
                }).start();
            }
        }
    }

    public static void setBosSplit(BufferedOutputStream bosSplit) {
        BigFileListener.bosSplit = bosSplit;
    }

    private HttpPost getHttpPost(String ipPort) {
        HttpPost httpPost = null;
        try {
            String url = "http://" + ipPort;
            httpPost = new HttpPost(url);
            RequestConfig config = RequestConfig.custom().setConnectionRequestTimeout(30000).setConnectTimeout(30000).setSocketTimeout(30000).build();
            httpPost.setConfig(config);
        } catch (Exception e) {
            logger.info(logger, e);
            System.exit(-1);
        }
        return httpPost;
    }

    private void senMsg(CloseableHttpClient httpClient, HttpPost httpPost, String message) {
        CloseableHttpResponse response;
        try {
            List<NameValuePair> params = new ArrayList<>();
            params.add(new BasicNameValuePair("msgjson", message));
            UrlEncodedFormEntity entity = new UrlEncodedFormEntity(params, "UTF-8");
            httpPost.setEntity(entity);
            response = httpClient.execute(httpPost);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                logger.info("错误信息为：" + +response.getStatusLine().getStatusCode());
            } else {
                HttpEntity entityJson = response.getEntity();
                String recvdata = EntityUtils.toString(entityJson, "UTF-8");
                logger.info("返回内容：[" + recvdata + "]");
            }
        } catch (Exception e) {
            logger.info("二进制流返回丢失信息失败！！！", e);
        }
    }

    private void stop() {
        IOUtils.closeQuietly(bos);
    }
}
