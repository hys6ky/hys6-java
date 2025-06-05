package hyren.serv6.agent.job.biz.core.dfstage.fileparser.impl;

import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.List;

@Slf4j
public class CsvFileParserDeal extends FileParserAbstract {

    public CsvFileParserDeal(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        super(tableBean, collectTableBean, readFile);
    }

    @Override
    public String parserFile() {
        long fileRowCount = 0;
        List<String> valueList;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(new File(readFile).toPath()), DataBaseCode.ofValueByCode(tableBean.getFile_code())));
            CsvListReader csvReader = new CsvListReader(br, CsvPreference.EXCEL_PREFERENCE)) {
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
                valueList = csvReader.read();
                if (valueList != null) {
                    log.info("读取到表头为：" + valueList);
                }
            }
            while ((valueList = csvReader.read()) != null) {
                fileRowCount++;
                checkData(valueList, fileRowCount);
                dealLine(valueList);
                if (fileRowCount % JobConstant.BUFFER_ROW == 0) {
                    writer.flush();
                    log.info("正在处理转存文件，已写入" + fileRowCount + "行");
                }
            }
            writer.flush();
        } catch (Exception e) {
            throw new AppSystemException("解析CSV文件转存报错", e);
        }
        return unloadFileAbsolutePath + Constant.METAINFOSPLIT + fileRowCount;
    }
}
