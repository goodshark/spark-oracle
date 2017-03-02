package org.apache.hadoop.hive.ql.io.orc;

/**
 * Created by wangsm9 on 2016/9/27.
 */
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
public   class SparkOrcStruct {

    public static  void setOrcStructValue(RecordIdentifier recordIdentifier,int filedIndex,OrcStruct value ){
        StringBuffer sb =new StringBuffer();
        sb.append(recordIdentifier.getTransactionId());
        sb.append("^");
        sb.append(recordIdentifier.getBucketId());
        sb.append("^");
        sb.append(recordIdentifier.getRowId());
        value.setFieldValue(filedIndex,sb.toString());
        value.setNumFields(filedIndex  + 1);
    }
}
