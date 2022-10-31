package parser;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;



public class JavaCc {
    public static void main(String[] args) throws Exception {
        String sql="select * from t_user where id=1";

        SqlParser.Config myConfig = SqlParser.config().withLex(Lex.MYSQL);
        SqlParser parser = SqlParser.create(sql, myConfig);
        SqlNode sqlNode = parser.parseQuery();
        System.out.println(sqlNode);
    }
}
