package hive.tsql.another;

import org.apache.hive.tsql.ProcedureCli;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.util.StrUtils;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhongdg1 on 2017/2/16.
 */
public class ExecuteStringStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_EXECUTE_STRING_";

    public ExecuteStringStatement() {
        super(STATEMENT_NAME);
    }

    private Set<String> vars = new HashSet<>();

    public void addLocalVars(Set<String> varNames) {
        vars.addAll(varNames);
    }

    @Override
    public int execute() throws Exception {
        try {
            ProcedureCli cli = new ProcedureCli(getExecSession().getSparkSession());
//            cli.setExecSession(getExecSession());
            cli.getExecSession().setVariableContainer(getExecSession().getVariableContainer());
            cli.getExecSession().setReset(false);
            cli.callProcedure(StrUtils.trimQuot(getExecSql()));
//            getExecSession().getResultSets().addAll(cli.getExecSession().getResultSets());
            int size = cli.getExecSession().getResultSets().size();
            if (size > 0) {
                this.setRs(cli.getExecSession().getResultSets().get(size - 1));
                getExecSession().getResultSets().addAll(cli.getExecSession().getResultSets());
            }

        } catch (Throwable e) {
            throw new RuntimeException("Executing # " + getSql(), e);
        }

        return 0;
    }


    public String getExecSql() throws Exception {
        String execSQL = getSql();
        if(execSQL.startsWith("N'")) {
            execSQL = execSQL.substring(1, execSQL.length());
        }
        if (!vars.isEmpty()) {
            for (String s : vars) {
                Var v = s.startsWith("@@") ? findSystemVar(s) : findVar(s);
                if (v == null) {
                    throw new NotDeclaredException(s);
                }
//                String value = v.getVarValue().toString();
                execSQL = execSQL.replaceAll(s, null == v.getVarValue() ? StrUtils.addQuot("") : StrUtils.addQuot(v.getVarValue().toString()));
            }
        }
        return execSQL;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
