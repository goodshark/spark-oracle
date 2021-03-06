package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.func.DateUnit;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public abstract class BaseCalculator implements Calculator {
    private List<Var> arguments = new ArrayList<>();
    private String funcName;
    private int minSize = 0;
    private int maxSize = Integer.MAX_VALUE;
    private int size = 0;
    private ExecSession execSession;
    private boolean checkNull = true;

    public BaseCalculator() {
    }


    public BaseCalculator setExecSession(ExecSession execSession) {
        this.execSession = execSession;
        return this;
    }

    public ExecSession getExecSession() {
        return execSession;
    }

    public BaseCalculator setArguments(List<Var> arguments) {
//        this.arguments = arguments;
        if (null == arguments || arguments.size() == 0) {
            return this;
        }
        for (Var var : arguments) {
            this.arguments.add(var.clone());
        }
        size = this.arguments.size();
        return this;
    }

    public void setCheckNull(boolean checkNull) {
        this.checkNull = checkNull;
    }

    public int getSize() {
        return size;
    }

    public void setMinSize(int minSize) {
        this.minSize = minSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public void setMinMax(int size) {
        this.maxSize = size;
        this.minSize = size;
    }

    public List<Var> getArguments() {
        return arguments;
    }

    public Var getArguments(int index) {
        return arguments.get(index);
    }

    public List<Var> getAllArguments() {
        return arguments;
    }

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    public boolean checkArguments() throws Exception {
        return size >= minSize && size <= maxSize;
    }

    public Var doComputing() throws Exception {
        if (!checkArguments()) {
            throw new FunctionArgumentException(funcName, size, minSize, maxSize);
        }
        if (checkNull) {
            checkNull();
        }

        return compute();
    }

    private void checkNull() throws Exception {
        if (null == arguments || arguments.isEmpty()) {
            return;
        }
        for (Var var : arguments) {
            if (null == var || null == var.getVarValue() || Var.DataType.NULL == var.getDataType()) {
                throw new IllegalArgumentException("Function Argument Is NULL");
            }
        }
    }

    public int getDatePartValue(DateUnit dateUnit, Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
//        calendar.setFirstDayOfWeek(Calendar.MONDAY);
//        calendar.setMinimalDaysInFirstWeek(4);
        int result = -1;
        switch (dateUnit) {
            case YEAR:
                result = calendar.get(Calendar.YEAR);
                break;
            case MONTH:
                result = calendar.get(Calendar.MONTH) + 1;
                break;
            case DAY:
                result = calendar.get(Calendar.DAY_OF_MONTH);
                break;
            case HOUR:
                result = calendar.get(Calendar.HOUR_OF_DAY);
                break;
            case MINUTE:
                result = calendar.get(Calendar.MINUTE);
                break;
            case SECOND:
                result = calendar.get(Calendar.SECOND);
                break;
            case QUARTER:
                result = (calendar.get(Calendar.MONTH) / 3) + 1;
                break;
            case DAYOFYEAR:
                result = calendar.get(Calendar.DAY_OF_YEAR);
                break;
            case WEEK:
                result = calendar.get(Calendar.WEEK_OF_YEAR);
                break;
            case WEEKDAY:
                result = calendar.get(Calendar.DAY_OF_WEEK);
                break;
            case MILLISECOND:
                result = calendar.get(Calendar.MILLISECOND);
            default:
                break;

        }
        return result;
    }



}
