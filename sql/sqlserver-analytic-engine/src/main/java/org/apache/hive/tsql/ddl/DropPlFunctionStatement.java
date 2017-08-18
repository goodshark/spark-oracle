package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.dbservice.PlFunctionService;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenfolin on 2017/8/18.
 */
public class DropPlFunctionStatement extends BaseStatement{

    private String db;
    private String name;
    private SparkSession sparkSession;

    public DropPlFunctionStatement(String db, String name, SparkSession sparkSession){
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
            if (name != null) {
                PlFunctionService service = PlFunctionService.getInstance(sparkSession.sparkContext().hadoopConfiguration().get(Common.DBURL),
                        sparkSession.sparkContext().hadoopConfiguration().get(Common.USER_NAME),
                        sparkSession.sparkContext().hadoopConfiguration().get(Common.PASSWORD));
                if(name.contains("*")){
                    List<PlFunctionRegistry.PlFunctionDescription> list = PlFunctionRegistry.getInstance().listOraclePlFunc(db, name);
                    for(PlFunctionRegistry.PlFunctionDescription f : list){
                        service.delPlFunction(f.getFunc(), PlFunctionService.ORACLE_FUNCTION_TYPE);
                        PlFunctionRegistry.getInstance().delOraclePlFunc(f.getFunc());
                    }
                } else {
                    service.delPlFunction(
                                    new PlFunctionRegistry.PlFunctionIdentify(db, name), PlFunctionService.ORACLE_FUNCTION_TYPE);
                    PlFunctionRegistry.getInstance().delOraclePlFunc(new PlFunctionRegistry.PlFunctionIdentify(db, name));
                }
            }
        }
        return 0;
    }
}
