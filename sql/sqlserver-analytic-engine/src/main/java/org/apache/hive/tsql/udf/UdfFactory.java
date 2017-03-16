package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.exception.FunctionNotFoundException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2017/1/20.
 */
public class UdfFactory {
    //<functionName, functionImplclassName>
    private final static ConcurrentHashMap<String, Class<BaseCalculator>> udfs = new ConcurrentHashMap<>();

    private UdfFactory() {
        try {
            //string function
            registFunction("len".toUpperCase(), "org.apache.hive.tsql.udf.string.LenCalculator");
            registFunction("substring".toUpperCase(), "org.apache.hive.tsql.udf.string.SubStringCalculator");
            registFunction("REVERSE".toUpperCase(), "org.apache.hive.tsql.udf.string.ReverseCalculator");
            registFunction("CONCAT".toUpperCase(), "org.apache.hive.tsql.udf.string.ConcatCalculator");
            registFunction("ASCII".toUpperCase(), "org.apache.hive.tsql.udf.string.AsciiCalculator");
            registFunction("TRIM".toUpperCase(), "org.apache.hive.tsql.udf.string.TrimCalculator");
            registFunction("LTRIM".toUpperCase(), "org.apache.hive.tsql.udf.string.LtrimCalculator");
            registFunction("RTRIM".toUpperCase(), "org.apache.hive.tsql.udf.string.RtrimCalculator");
            registFunction("LOWER".toUpperCase(), "org.apache.hive.tsql.udf.string.LowerCalculator");
            registFunction("UPPER".toUpperCase(), "org.apache.hive.tsql.udf.string.UpperCalculator");
            registFunction("SPACE".toUpperCase(), "org.apache.hive.tsql.udf.string.SpaceCalculator");
            registFunction("REPLICATE".toUpperCase(), "org.apache.hive.tsql.udf.string.ReplicateCalculator");
            registFunction("SOUNDEX".toUpperCase(), "org.apache.hive.tsql.udf.string.SoundexCalculator");
            registFunction("CHARINDEX".toUpperCase(), "org.apache.hive.tsql.udf.string.CharindexCalculator");
            registFunction("STRING_SPLIT".toUpperCase(), "org.apache.hive.tsql.udf.string.StringSplitCalculator");
            registFunction("CONCAT_WS".toUpperCase(), "org.apache.hive.tsql.udf.string.ConcatWsCalculator");
            registFunction("TRANSLATE".toUpperCase(), "org.apache.hive.tsql.udf.string.TranslateCalculator");
            registFunction("STUFF".toUpperCase(), "org.apache.hive.tsql.udf.string.StuffCalculator");
            registFunction("REPLACE".toUpperCase(), "org.apache.hive.tsql.udf.string.ReplaceCalculator");
            registFunction("CHAR".toUpperCase(), "org.apache.hive.tsql.udf.string.CharCalculator");
            registFunction("LEFT".toUpperCase(), "org.apache.hive.tsql.udf.string.SubStringCalculator");
            registFunction("RIGHT".toUpperCase(), "org.apache.hive.tsql.udf.string.SubStringCalculator");
            registFunction("PATINDEX".toUpperCase(), "org.apache.hive.tsql.udf.string.PatIndexCalculator");
            //cursor function
            registFunction("CURSOR_STATUS".toUpperCase(), "org.apache.hive.tsql.udf.CursorStatusCalculator");
            //datatype function
            registFunction("cast".toUpperCase(), "org.apache.hive.tsql.udf.CastCalculator");
            registFunction("convert".toUpperCase(), "org.apache.hive.tsql.udf.ConvertCalculator");
            //date function
            registFunction("getdate".toUpperCase(), "org.apache.hive.tsql.udf.date.GetDateCalculator");
            registFunction("SYSDATETIME".toUpperCase(), "org.apache.hive.tsql.udf.date.GetDateCalculator");
            registFunction("SYSDATETIMEOFFSET".toUpperCase(), "org.apache.hive.tsql.udf.date.GetDateCalculator");
            registFunction("DATEADD".toUpperCase(), "org.apache.hive.tsql.udf.date.DateAddCalculator");
            registFunction("DATEDIFF".toUpperCase(), "org.apache.hive.tsql.udf.date.DateDiffCalculator");
            registFunction("DATENAME".toUpperCase(), "org.apache.hive.tsql.udf.date.DateNameCalculator");
            registFunction("year".toUpperCase(), "org.apache.hive.tsql.udf.date.YearCalculator");
            registFunction("month".toUpperCase(), "org.apache.hive.tsql.udf.date.MonthCalculator");
            registFunction("day".toUpperCase(), "org.apache.hive.tsql.udf.date.DayCalculator");
            registFunction("minute".toUpperCase(), "org.apache.hive.tsql.udf.date.MinuteCalculator");
            registFunction("hour".toUpperCase(), "org.apache.hive.tsql.udf.date.HourCalculator");
            registFunction("second".toUpperCase(), "org.apache.hive.tsql.udf.date.SecondCalculator");
            registFunction("quarter".toUpperCase(), "org.apache.hive.tsql.udf.date.QuarterCalculator");
            registFunction("dayofyear".toUpperCase(), "org.apache.hive.tsql.udf.date.DayOfYearCalculator");
            registFunction("isdate".toUpperCase(), "org.apache.hive.tsql.udf.date.IsDateCalculator");

            //math functions
            registFunction("ABS".toUpperCase(), "org.apache.hive.tsql.udf.math.AbsCalculator");
            registFunction("ACOS".toUpperCase(), "org.apache.hive.tsql.udf.math.AcosCalculator");
            registFunction("ASIN".toUpperCase(), "org.apache.hive.tsql.udf.math.AsinCalculator");
            registFunction("ATN2".toUpperCase(), "org.apache.hive.tsql.udf.math.Atan2Calculator");
            registFunction("ATAN".toUpperCase(), "org.apache.hive.tsql.udf.math.AtanCalculator");
            registFunction("CEILING".toUpperCase(), "org.apache.hive.tsql.udf.math.CeilingCalculator");
            registFunction("COS".toUpperCase(), "org.apache.hive.tsql.udf.math.CosCalculator");
            registFunction("DEGREES".toUpperCase(), "org.apache.hive.tsql.udf.math.DegreesCalculator");
            registFunction("EXP".toUpperCase(), "org.apache.hive.tsql.udf.math.ExpCalculator");
            registFunction("FLOOR".toUpperCase(), "org.apache.hive.tsql.udf.math.FloorCalculator");
            registFunction("LOG10".toUpperCase(), "org.apache.hive.tsql.udf.math.Log10Calculator");
            registFunction("LOG".toUpperCase(), "org.apache.hive.tsql.udf.math.LogCalculator");
            registFunction("PI".toUpperCase(), "org.apache.hive.tsql.udf.math.PiCalculator");
            registFunction("POWER".toUpperCase(), "org.apache.hive.tsql.udf.math.PowerCalculator");
            registFunction("RAND".toUpperCase(), "org.apache.hive.tsql.udf.math.RandCalculator");
            registFunction("ROUND".toUpperCase(), "org.apache.hive.tsql.udf.math.RoundCalculator");
            registFunction("SIGN".toUpperCase(), "org.apache.hive.tsql.udf.math.SignCalculator");
            registFunction("SIN".toUpperCase(), "org.apache.hive.tsql.udf.math.SinCalculator");
            registFunction("SQRT".toUpperCase(), "org.apache.hive.tsql.udf.math.SqrtCalculator");
            registFunction("SQUARE".toUpperCase(), "org.apache.hive.tsql.udf.math.SquareCalculator");
            registFunction("TAN".toUpperCase(), "org.apache.hive.tsql.udf.math.TanCalculator");
            //others
            registFunction("NULLIF".toUpperCase(), "org.apache.hive.tsql.udf.NullIfCalculator");
            registFunction("isnull".toUpperCase(), "org.apache.hive.tsql.udf.IsNullCalculator");
            registFunction("OBJECT_ID".toUpperCase(), "org.apache.hive.tsql.udf.ObjectIdCalculator");
            registFunction("Coalesce".toUpperCase(), "org.apache.hive.tsql.udf.CoalesceCalculator");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UdfFactoryHolder {
        private final static UdfFactory factory = new UdfFactory();
    }

    public static UdfFactory getUdfFactory() {
        return UdfFactoryHolder.factory;
    }


    public BaseCalculator createCalculator(String funcName) throws Exception {
        Class<BaseCalculator> functionClz = udfs.get(funcName);
        if (null == functionClz) {
            throw new FunctionNotFoundException(funcName);
        }
        BaseCalculator cal = functionClz.newInstance();
        cal.setFuncName(funcName);
        return cal;
    }

    public void registFunction(String funcName, String clzName) throws ClassNotFoundException {
        udfs.put(funcName.trim(), (Class<BaseCalculator>) Class.forName(clzName));
    }

    public static boolean funcExists(String funcName) {
        return udfs.containsKey(funcName);
    }
}
