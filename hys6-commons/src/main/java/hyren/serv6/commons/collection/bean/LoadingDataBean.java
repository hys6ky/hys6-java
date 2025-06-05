package hyren.serv6.commons.collection.bean;

import fd.ng.core.annotation.DocClass;
import java.util.Map;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/4/22 0022 下午 01:43")
public class LoadingDataBean {

    private Map<String, String> layerMap;

    private boolean isBatch = true;

    private boolean isDirTran = true;

    private int batchNum = 50000;

    private String tableName;

    public Map<String, String> getLayerMap() {
        return layerMap;
    }

    public void setLayerMap(Map<String, String> layerMap) {
        this.layerMap = layerMap;
    }

    public boolean isBatch() {
        return isBatch;
    }

    public void setBatch(boolean batch) {
        isBatch = batch;
    }

    public boolean isDirTran() {
        return isDirTran;
    }

    public void setDirTran(boolean dirTran) {
        isDirTran = dirTran;
    }

    public int getBatchNum() {
        return batchNum;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
