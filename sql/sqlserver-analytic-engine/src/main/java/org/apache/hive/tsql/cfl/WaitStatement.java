package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class WaitStatement extends BaseStatement {

    private TreeNode expr = null;
    private boolean delay = false;

    public WaitStatement() {
    }

    public WaitStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void addExpr(TreeNode expression) {
        expr = expression;
    }

    public void setDelay() {
        delay = true;
    }

    private long evalDelayTime(String str) {
        try {
            String timestr = str.trim();
            timestr = timestr.replace('\'', ' ');
            timestr = timestr.trim();
            if (delay) {
                // sqlserver accept three digits, here only accept 2 digits
                Pattern pa = Pattern.compile("([0-9]{2}):([0-9]{2})");
                Matcher matcher = pa.matcher(timestr);
                if (matcher.matches() && matcher.groupCount() == 2) {
                    // TODO hours can NOT more than 24
                    int hours = Integer.parseInt(matcher.group(1));
                    int mins = Integer.parseInt(matcher.group(2));
                    return hours * 3600000 + mins * 60000;
                }
            } else {
                Calendar cal = Calendar.getInstance();
                int year = cal.get(Calendar.YEAR);
                int month = cal.get(Calendar.MONTH) + 1;
                int day = cal.get(Calendar.DAY_OF_MONTH);
                String dateStr = Integer.toString(year) + "-" + Integer.toString(month) + "-" + Integer.toString(day);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Date date = sdf.parse(dateStr + " " + timestr);
                long diffMs =  date.getTime() - cal.getTime().getTime();
                if (diffMs >= 0)
                    return diffMs;
                else {
                    // execute until reach the TIME on next day
                    return 24*3600*1000 + diffMs;
                }
            }
        } catch (Exception e) {
            System.out.println("eval delay error");
            e.printStackTrace();
        }
        return 0;
    }

    public int execute() throws Exception {
        try {
            expr.execute();
            ResultSet exprRes = expr.getRs();
            Var exprVar = (Var) exprRes.getObject(0);
            if (exprVar.getDataType() != Var.DataType.STRING)
                return -1;
            String timeStr = (String) exprVar.getVarValue();
            long delayMS = evalDelayTime(timeStr);
            System.out.println("dealy time MSsec: " + delayMS);
            Thread.sleep(delayMS);
        } catch (Exception e) {
            System.out.println("waitfor get error");
            e.printStackTrace();
        }
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    public String toString() {
        return "WAITFOR " + (delay ? "DELAY " : "TIME ") + expr.toString();
    }
}
