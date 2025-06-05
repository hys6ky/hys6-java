package hyren.serv6.h.process.spark.dataset;

import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.ProcessType;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.entity.DmJobTableFieldInfo;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.process.bean.ProcessJobTableConfBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class AbsDataSetProcesser implements DataSetProcesser {

    private final ProcessJobTableConfBean processJobTableConfBean;

    protected List<String> partitionFields = new ArrayList<>();

    protected List<String> primaryKeyInfos = new ArrayList<>();

    protected AbsDataSetProcesser(ProcessJobTableConfBean processJobTableConfBean) {
        this.processJobTableConfBean = processJobTableConfBean;
        processJobTableConfBean.getFieldAdditionalInfoMap().forEach((k, v) -> {
            StoreLayerAdded sla = StoreLayerAdded.ofEnumByCode(k);
            if (sla == StoreLayerAdded.ZhuJian) {
                this.primaryKeyInfos.addAll(v);
            }
            if (sla == StoreLayerAdded.FenQuLie) {
                this.partitionFields.addAll(v);
            }
        });
    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataSet) throws Exception {
        List<DmJobTableFieldInfo> fields = getNeedToHandleFields();
        String[] originalSqlcolumnName = dataSet.columns();
        List<String> keepColumnName = new ArrayList<>();
        Column[] sqlColumn = new Column[fields.size()];
        List<String> columnForMD5 = new ArrayList<>();
        StorageType storageType = StorageType.ofEnumByCode(processJobTableConfBean.getDmModuleTable().getStorage_type());
        for (int i = 0, n = 0; i < fields.size(); i++) {
            DmJobTableFieldInfo field = fields.get(i);
            String columnName = field.getJobtab_field_en_name();
            String processCode = field.getJobtab_field_process();
            if (ProcessType.ZiZeng.getCode().equals(processCode)) {
                sqlColumn[i] = functions.monotonically_increasing_id();
            } else if (ProcessType.YingShe.getCode().equals(processCode) || ProcessType.FenZhuYingShe.getCode().equals(processCode) || ProcessType.HanShuYingShe.getCode().equals(processCode)) {
                sqlColumn[i] = new Column(originalSqlcolumnName[n]);
                keepColumnName.add(originalSqlcolumnName[n]);
                if (storageType == StorageType.LiShiLaLian || storageType == StorageType.UpSet) {
                    processJobTableConfBean.getFieldAdditionalInfoMap().forEach((k, v) -> {
                        StoreLayerAdded sla = StoreLayerAdded.ofEnumByCode(k);
                        if (sla == StoreLayerAdded.ZhuJian) {
                            v.forEach(column -> {
                                if (column.equalsIgnoreCase(columnName)) {
                                    columnForMD5.add(columnName);
                                    primaryKeyInfos.add(columnName);
                                }
                            });
                        }
                        if (sla == StoreLayerAdded.FenQuLie) {
                            partitionFields.add(columnName);
                        }
                    });
                } else {
                    columnForMD5.add(columnName);
                }
                n++;
            } else if (ProcessType.DingZhi.getCode().equals(processCode)) {
                sqlColumn[i] = new Column(field.getJobtab_process_mapping());
            } else {
                throw new Exception(String.format("字段: [ %s ],不支持处理方式码: [ %s ]", columnName, processCode));
            }
            sqlColumn[i] = sqlColumn[i].name(columnName.toUpperCase());
        }
        processJobTableConfBean.setPartitionFields(partitionFields);
        log.info("original partition fields: [ {} ]", JsonUtil.toJson(partitionFields));
        processJobTableConfBean.setPrimaryKeyInfos(primaryKeyInfos);
        log.info("original primaryKeyInfos fields: [ {} ]", JsonUtil.toJson(primaryKeyInfos));
        log.info("original calc columnForMD`5: [ {} ]", JsonUtil.toJson(columnForMD5));
        if (storageType == StorageType.LiShiLaLian || storageType == StorageType.UpSet) {
            if (columnForMD5.isEmpty()) {
                throw new Exception("进数方式为 F3,F5,Upset 时,用来计算MD5的字段列表不能为空!");
            }
        }
        List<Column> finalColumnForMD5 = new ArrayList<>();
        List<String> final_columnForMD5 = columnForMD5.stream().sorted().collect(Collectors.toList());
        final_columnForMD5.forEach(column -> finalColumnForMD5.add(functions.concat(functions.coalesce(new Column(column), functions.lit("")), functions.lit("^@#"))));
        log.info("table: [ {} ], storageType: [ {} ], final calc finalColumnForMD5: [ {} ]", processJobTableConfBean.getTarTableName(), storageType.getValue(), JsonUtil.toJson(final_columnForMD5));
        String dropColumns = Arrays.stream(originalSqlcolumnName).filter(s -> !keepColumnName.contains(s)).collect(Collectors.joining(","));
        dataSet = dataSet.drop(dropColumns).select(sqlColumn);
        Column md5_str = functions.md5(functions.concat(finalColumnForMD5.toArray(new Column[0])));
        log.info("md5_str: " + md5_str);
        dataSet = dataSet.withColumn(Constant._HYREN_JOB_NAME, functions.lit(processJobTableConfBean.getJobNameParam()));
        if (processJobTableConfBean.getIsZipperFlag() == IsFlag.Shi) {
            dataSet = dataSet.withColumn(Constant._HYREN_S_DATE, functions.lit(processJobTableConfBean.getEtlDate()));
            dataSet = dataSet.withColumn(Constant._HYREN_E_DATE, functions.lit(Constant._MAX_DATE_8));
            dataSet = dataSet.withColumn(Constant._HYREN_MD5_VAL, md5_str);
        } else {
            dataSet = dataSet.withColumn(Constant._HYREN_S_DATE, functions.lit(processJobTableConfBean.getEtlDate()));
            if (storageType == StorageType.UpSet) {
                dataSet = dataSet.withColumn(Constant._HYREN_MD5_VAL, md5_str);
            }
        }
        List<Column> finalSQLColumns = new ArrayList<>();
        String[] columns = dataSet.columns();
        if (partitionFields.isEmpty()) {
            for (String column : columns) {
                finalSQLColumns.add(new Column(column));
            }
        } else {
            for (String column : columns) {
                for (String partitionField : partitionFields) {
                    if (partitionField.equalsIgnoreCase(column))
                        continue;
                    finalSQLColumns.add(new Column(column));
                }
            }
        }
        for (String partitionField : partitionFields) {
            finalSQLColumns.add(new Column(partitionField));
        }
        Column[] finalColumns = finalSQLColumns.toArray(new Column[0]);
        dataSet = dataSet.select(finalColumns);
        dataSet.printSchema();
        return dataSet;
    }

    private List<DmJobTableFieldInfo> getNeedToHandleFields() {
        List<DmJobTableFieldInfo> needToHandleFields = new ArrayList<>();
        for (DmJobTableFieldInfo field : processJobTableConfBean.getDmJobTableFieldInfos()) {
            if (!Constant.HYRENFIELD.contains(field.getJobtab_field_en_name().toUpperCase())) {
                needToHandleFields.add(field);
            }
        }
        return needToHandleFields;
    }
}
