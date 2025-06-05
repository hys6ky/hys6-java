package hyren.serv6.hadoop.agent.biz.bulkload;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.hadoop.commons.hadoop_helper.HBaseOperator;
import hyren.serv6.hadoop.utils.BKLoadUtil;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcInputFormat;
import org.apache.parquet.hadoop.ParquetInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@DocClass(author = "zxz", desc = "", createdate = "2020/07/17")
@Slf4j
public class OrcBulkLoadJob extends Configured implements Tool {

    public static class BulkLoadMap extends Mapper<NullWritable, OrcStruct, ImmutableBytesWritable, Put> {

        private List<byte[]> headByte = null;

        private List<Integer> rowKeyIndex = null;

        private boolean isMd5 = false;

        private final StringBuilder sb = new StringBuilder();

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            List<String> columnList = StringUtil.split(conf.get("columnMetaInfo"), Constant.METAINFOSPLIT);
            headByte = new ArrayList<>(columnList.size());
            columnList.forEach(column -> headByte.add(column.getBytes()));
            isMd5 = conf.get("isMd5").equals(IsFlag.Shi.getCode());
            List<String> rowKeyIndexList = StringUtil.split(conf.get("rowKeyIndex"), Constant.METAINFOSPLIT);
            rowKeyIndex = new ArrayList<>(rowKeyIndexList.size());
            rowKeyIndexList.forEach(index -> rowKeyIndex.add(Integer.valueOf(index)));
        }

        public void map(NullWritable key, OrcStruct value, Context context) throws IOException, InterruptedException {
            String row_key;
            if (isMd5) {
                for (int index : rowKeyIndex) {
                    sb.append(value.getFieldValue(index));
                }
                row_key = DigestUtils.md5Hex(sb.toString());
            } else {
                for (int index : rowKeyIndex) {
                    sb.append(value.getFieldValue(index));
                }
                row_key = sb.toString();
            }
            sb.delete(0, sb.length());
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(row_key.getBytes());
            Put put = new Put(Bytes.toBytes(row_key));
            for (int i = 0; i < headByte.size(); i++) {
                put.addColumn(Constant.HBASE_COLUMN_FAMILY, headByte.get(i), Bytes.toBytes(value.getFieldValue(i).toString()));
            }
            context.write(rowkey, put);
        }
    }

    public int run(String[] args) throws Exception {
        String todayTableName = args[0];
        String hdfsFilePath = args[1];
        String columnMetaInfo = args[2];
        String rowKeyIndex = args[3];
        String configPath = args[4];
        String etlDate = args[5];
        String isMd5 = args[6];
        String hadoop_user_name = args[7];
        String platform = args[8];
        String prncipal_name = args[9];
        log.info("Arguments: " + todayTableName + "  " + hdfsFilePath + "  " + columnMetaInfo + "  " + rowKeyIndex + "  " + configPath + "  " + etlDate + "  " + isMd5 + "  " + hadoop_user_name + "  " + platform + "  " + prncipal_name);
        try (HBaseOperator hbaseOperator = new HBaseOperator(configPath, platform, prncipal_name, hadoop_user_name)) {
            hbaseOperator.conf.set("columnMetaInfo", columnMetaInfo);
            hbaseOperator.conf.set("etlDate", etlDate);
            hbaseOperator.conf.set("isMd5", isMd5);
            hbaseOperator.conf.set("rowKeyIndex", rowKeyIndex);
            Job job = Job.getInstance(hbaseOperator.conf, "OrcBulkLoadJob_" + todayTableName);
            job.setJarByClass(OrcBulkLoadJob.class);
            job.setInputFormatClass(OrcInputFormat.class);
            ParquetInputFormat.setInputPaths(job, hdfsFilePath);
            String outputPath = JobConstant.TMPDIR + "/bulkload/output" + System.currentTimeMillis();
            Path tmpPath = new Path(outputPath);
            FileOutputFormat.setOutputPath(job, tmpPath);
            job.setMapperClass(BulkLoadMap.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);
            int resultCode = BKLoadUtil.jobOutLoader(todayTableName, tmpPath, hbaseOperator, job);
            try (FileSystem fs = FileSystem.get(hbaseOperator.conf)) {
                fs.delete(tmpPath, true);
            }
            return resultCode;
        }
    }
}
