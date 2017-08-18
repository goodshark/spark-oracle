package org.apache.hive.tsql.dml;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;

import java.util.List;

/**
 * Created by chenfolin on 2017/8/17.
 */
public class ShowPlFunctionStatement extends BaseStatement{

    private String db;
    private String name;
    private SparkSession sparkSession;

    public ShowPlFunctionStatement(String db, String name, SparkSession sparkSession){
        this.db = db;
        this.name = name;
        this.sparkSession = sparkSession;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        if (db == null) {
            this.db = sparkSession.sessionState().catalog().getCurrentDatabase();
        }
        if("oracle".equalsIgnoreCase(sparkSession.getSessionState().conf().getConfString("spark.sql.analytical.engine","spark"))){
            if (name == null) {
                List<String> funcs = PlFunctionRegistry.getInstance().listOraclePlFunc(db);
                SparkResultSet resultSet = new SparkResultSet();
                for (String str : funcs) {
                    resultSet.addRow(new Object[]{new Var("Pl Function Name", str, Var.DataType.STRING)});
                }
                setRs(resultSet);
            } else {
                List<PlFunctionRegistry.PlFunctionDescription> funcs = PlFunctionRegistry.getInstance().listOraclePlFunc(db, name);
                SparkResultSet resultSet = new SparkResultSet();
                for (PlFunctionRegistry.PlFunctionDescription func : funcs) {
                    resultSet.addRow(new Object[]{new Var("Pl Function Name", func.getFunc().getName(), Var.DataType.STRING), new Var("Pl Function Sql", func.getBody(), Var.DataType.STRING)});
                }
                setRs(resultSet);
            }
        }
        return 0;
    }

}
