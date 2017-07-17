package org.apache.hive.plsql.testsql;

/**
 * Created by wangsm9 on 2017/7/12.
 */
public class SelectSqlCase {

    public static final String SLECT_SQL_001 = "select * from tb001";
    public static final String SLECT_SQL_002 = "select id,name from tb001";
    public static final String SLECT_SQL_003 = "select id,name from tb001 where id = 1";


    public static final String SELECT_SQL_02 = "create OR replace PROCEDURE foobar(p1 integer := 1) AS\n" +
            "BEGIN\n" +
            "\tdbms_output.put_line('p: ' || p1);\n" +
            "end;\n" +
            "\n" +
            "BEGIN\n" +
            "\tfoobar(2);\n" +
            "END;\n";


    public static final String TEST_SQL_01 = "DECLARE\n" +
            "\tx INTEGER := 100;\n" +
            "\tstartindex INTEGER := 1;\n" +
            "BEGIN\n" +
            "\tfor a in startindex..2 loop\n" +
            "\t\tdbms_output.put_line('loop1 start');\n" +
            "\t\t<<block1>>\n" +
            "\t\tDECLARE\n" +
            "\t\t\tx INTEGER := 200;\n" +
            "\t\t\ty INTEGER := 300;\n" +
            "\t\tBEGIN\n" +
            "\t\t\tdbms_output.put_line('loop1 1 inner block: ' || a  || ' ' || x  || ' ' || y);\n" +
            "\t\t\t<<block2>>\n" +
            "\t\t\tDECLARE\n" +
            "\t\t\t\tx INTEGER := 400;\n" +
            "\t\t\t\ty INTEGER := 500;\n" +
            "\t\t\tBEGIN\n" +
            "\t\t\t\tdbms_output.put_line('loop1 2 inner block: ' || block1.x || ' ' || y);\n" +
            "\t\t\tEND;\n" +
            "\t\t\t<<blockloop1>>\n" +
            "\t\t\t<<blockloop10>>\n" +
            "\t\t\t<<blockloop100>>\n" +
            "\t\t\tfor a in 1..100 loop\n" +
            "\t\t\t\t<<blockloop2>>\n" +
            "\t\t\t\tloop\n" +
            "\t\t\t\t\tdbms_output.put_line('loop1 inner loop: ' || a || ' ' || x || ' ' || y);\n" +
            "\t\t\t\t\texit blockloop10 when 1 = 1;\n" +
            "\t\t\t\tend loop;\n" +
            "\t\t\tend loop;\n" +
            "\t\tEND;\n" +
            "\t\tif x = 100 THEN\n" +
            "\t\t\tcontinue when x >= 100;\n" +
            "\t\tend if;\n" +
            "\t\tdbms_output.put_line('after continue');\n" +
            "  end loop;\n" +
            "\tdbms_output.put_line('loop1 finish');\n" +
            "end;";
    public static final String TEST_SQL = "<<topblock>>\n" +
            "DECLARE\n" +
            "\tx INTEGER := 1;\n" +
            "\tstartindex INTEGER := 1;\n" +
            "BEGIN\n" +
            "\tfor a in topblock.startindex..2 loop\n" +
            "\t\tdbms_output.put_line('loop1 start');\n" +
            "\t\t<<block1>>\n" +
            "\t\tDECLARE\n" +
            "\t\t\tx INTEGER := 200;\n" +
            "\t\t\ty INTEGER := 300;\n" +
            "\t\tBEGIN\n" +
            "\t\t\tdbms_output.put_line('loop1 1 inner block: ' || a || ' ' || x || ' ' || y);\n" +
            "\t\t\t<<block2>>\n" +
            "\t\t\tDECLARE\n" +
            "\t\t\t\tx INTEGER := 400;\n" +
            "\t\t\t\ty INTEGER := 500;\n" +
            "\t\t\tBEGIN\n" +
            "\t\t\t\tdbms_output.put_line('loop1 2 inner block: ' || block1.x || ' ' || y);\n" +
            "\t\t\tEND;\n" +
            "\t\t\t<<blockloop1>>\n" +
            "\t\t\t<<blockloop10>>\n" +
            "\t\t\t<<blockloop100>>\n" +
            "\t\t\tfor a in x..100 loop\n" +
            "\t\t\t\t<<blockloop2>>\n" +
            "\t\t\t\tloop\n" +
            "\t\t\t\t\tdbms_output.put_line('loop1 inner loop: ' || a || ' ' || x || ' ' || y);\n" +
            "\t\t\t\t\texit blockloop10 when 1 = 1;\n" +
            "\t\t\t\tend loop;\n" +
            "\t\t\tend loop;\n" +
            "\t\tEND;\n" +
            "\t\tif x = 100 THEN\n" +
            "\t\t\tcontinue when x >= 100;\n" +
            "\t\tend if;\n" +
            "\t\tdbms_output.put_line('after continue');\n" +
            "  end loop;\n" +
            "\tdbms_output.put_line('loop1 finish');\n" +
            "end;";
}
