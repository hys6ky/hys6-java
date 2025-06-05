package hyren.serv6.hadoop.commons.imp.fileparser.impl;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.List;

@Slf4j
public class SequenceFileParserDeal extends FileParserAbstract {

    public SequenceFileParserDeal(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        super(tableBean, collectTableBean, readFile);
    }

    @Override
    public String parserFile() {
        HdfsOperator hdfsOperator = new HdfsOperator();
        hdfsOperator.conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hdfsOperator.conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        hdfsOperator.conf.set("fs.defaultFS", "file:///");
        long fileRowCount = 0L;
        SequenceFile.Reader sfr = null;
        try {
            SequenceFile.Reader.Option optionFile = SequenceFile.Reader.file((new Path(readFile)));
            sfr = new SequenceFile.Reader(hdfsOperator.conf, optionFile);
            NullWritable key = NullWritable.get();
            Text value = new Text();
            List<String> valueList;
            while (sfr.next(key, value)) {
                fileRowCount++;
                String str = value.toString();
                valueList = StringUtil.split(str, Constant.SEQUENCEDELIMITER);
                checkData(valueList, fileRowCount);
                dealLine(valueList);
                if (fileRowCount % JobConstant.BUFFER_ROW == 0) {
                    writer.flush();
                    log.info("正在处理转存文件，已写入" + fileRowCount + "行");
                }
            }
            writer.flush();
        } catch (Exception e) {
            throw new AppSystemException("DB文件采集解析sequenceFile文件失败");
        } finally {
            try {
                if (sfr != null)
                    sfr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return unloadFileAbsolutePath + Constant.METAINFOSPLIT + fileRowCount;
    }
}
