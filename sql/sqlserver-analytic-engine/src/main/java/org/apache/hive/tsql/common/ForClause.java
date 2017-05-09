package org.apache.hive.tsql.common;

/**
 * Created by zhongdg1 on 2017/5/4.
 */
public class ForClause {
    public enum XFORMAT {XML, BROWSE}

    public enum XMLMODE {AUTO, PATH, RAW, EXPLICIT}

    public enum DIRECTIVES {BINARY, TYPE, ROOT, NONE}

    private XFORMAT xformat = XFORMAT.BROWSE;
    private XMLMODE xmlMode = XMLMODE.PATH;
    private DIRECTIVES directives = DIRECTIVES.NONE;
    private String row = "row";

    public XFORMAT getXformat() {
        return xformat;
    }


    public String getRow() {
        return row;
    }

    public void setRow(String row) {
        this.row = row;
    }

    public void setXformat(XFORMAT xformat) {
        this.xformat = xformat;
    }

    public XMLMODE getXmlMode() {
        return xmlMode;
    }

    public void setXmlMode(XMLMODE xmlMode) {
        this.xmlMode = xmlMode;
    }

    public DIRECTIVES getDirectives() {
        return directives;
    }

    public void setDirectives(DIRECTIVES directives) {
        this.directives = directives;
    }
}
