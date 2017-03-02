package hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class Position {
    private int startLine;
    private int stopLine;
    private int startIndex;
    private int stopIndex;

    public Position(int startLine, int startIndex, int stopLine, int stopIndex) {
        this.startLine = startLine;
        this.stopLine = stopLine;
        this.startIndex = startIndex;
        this.stopIndex = stopIndex;
    }

    @Override
    public String toString() {
        return new StringBuffer().append(" at line ").append("[").append(this.startLine).append(":").append(startIndex)
                .append(" -> ").append(stopLine).append(":").append(stopIndex).append("]").toString();
    }
}
