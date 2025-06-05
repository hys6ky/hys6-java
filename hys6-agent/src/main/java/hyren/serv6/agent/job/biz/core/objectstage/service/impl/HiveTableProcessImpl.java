package hyren.serv6.agent.job.biz.core.objectstage.service.impl;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.MD5Util;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.objectstage.service.ObjectProcessAbstract;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.*;
import java.util.List;
import java.util.Map;

public class HiveTableProcessImpl extends ObjectProcessAbstract {

    private BufferedWriter writer;

    public HiveTableProcessImpl(TableBean tableBean, ObjectTableBean objectTableBean) {
        super(tableBean, objectTableBean);
        if (isZipperKeyMap.isEmpty()) {
            for (String column : selectColumnList) {
                isZipperKeyMap.put(column, true);
            }
        }
        String unloadFileAbsolutePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + objectTableBean.getOdc_id() + File.separator + objectTableBean.getHyren_name() + File.separator + objectTableBean.getEtlDate() + File.separator + objectTableBean.getHyren_name() + ".dat", true);
        try {
            FileUtil.forceMkdir(new File(FileNameUtils.getFullPath(unloadFileAbsolutePath)));
            this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(unloadFileAbsolutePath), DataBaseCode.ofValueByCode(tableBean.getDbFileArchivedCode())));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void parserFileToTable(String readFile) {
        String lineValue;
        String code = DataBaseCode.ofValueByCode(objectTableBean.getDatabase_code());
        long num = 0;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(readFile)), code))) {
            StringBuilder md5_sb = new StringBuilder();
            StringBuilder value_sb = new StringBuilder();
            while ((lineValue = br.readLine()) != null) {
                List<Map<String, Object>> listTiledAttributes = getListTiledAttributes(lineValue, num);
                for (Map<String, Object> map : listTiledAttributes) {
                    if (!"delete".equals(map.get(operate_column))) {
                        num++;
                        for (String key : selectColumnList) {
                            if (isZipperKeyMap.get(key)) {
                                md5_sb.append(map.get(key));
                            }
                            value_sb.append(map.get(key)).append(Constant.DATADELIMITER);
                        }
                        value_sb.append(etlDate).append(Constant.DATADELIMITER);
                        value_sb.append(Constant._MAX_DATE_8).append(Constant.DATADELIMITER);
                        value_sb.append(MD5Util.md5String(md5_sb.toString()));
                        writer.write(value_sb.toString());
                        writer.write(Constant.DEFAULTLINESEPARATOR);
                        value_sb.delete(0, value_sb.length());
                        md5_sb.delete(0, md5_sb.length());
                    }
                }
                if (num > JobConstant.BUFFER_ROW) {
                    writer.flush();
                }
            }
            writer.flush();
        } catch (Exception e) {
            throw new AppSystemException("解析半结构化对象文件报错", e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
