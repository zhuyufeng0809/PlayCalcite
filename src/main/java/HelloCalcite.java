import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Frameworks;

import java.util.Collections;

import static org.apache.calcite.rel.rules.CoreRules.FILTER_TO_CALC;

public class HelloCalcite {
    public static void main(String[] args) throws Exception {
        String sql =
                "SELECT a.id, a.name, SUM(b.score1 * 0.7 + b.score2 * 0.3) AS total_score "
                        + "FROM student a "
                        + "INNER JOIN exam_result b ON a.id = b.student_id "
                        + "WHERE a.age < 20 AND b.score1 > 60.0 "
                        + "GROUP BY a.id, a.name";

        // Step 1. Parse
        SqlParser parser = SqlParser.create(sql);
        SqlNode originalSqlNode = parser.parseStmt();
        // Step 2. Validate
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add(
                "student",
                new AbstractTable() { // note: add a table
                    @Override
                    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                        RelDataTypeFactory.Builder builder = typeFactory.builder();

                        builder.add(
                                "ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                        builder.add("NAME", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.CHAR));
                        builder.add(
                                "AGE", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                        return builder.build();
                    }
                });

        rootSchema.add(
                "exam_result",
                new AbstractTable() {
                    @Override
                    public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
                        RelDataTypeFactory.Builder builder = typeFactory.builder();

                        builder.add(
                                "STUDENT_ID", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                        builder.add("SCORE1", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                        builder.add(
                                "SCORE2", new BasicSqlType(new RelDataTypeSystemImpl() {}, SqlTypeName.INTEGER));
                        return builder.build();
                    }
                });

        CalciteConnectionConfig readerConfig =
                CalciteConnectionConfig.DEFAULT.set(CalciteConnectionProperty.CASE_SENSITIVE, "false");
        SqlTypeFactoryImpl defaultTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        CalciteCatalogReader catalogReader =
                new CalciteCatalogReader(
                        CalciteSchema.from(rootSchema),
                        Collections.emptyList(),
                        defaultTypeFactory,
                        readerConfig);

        SqlValidator validator =
                SqlValidatorUtil.newValidator(
                        // allow Calcite to know which operators are valid in the system
                        SqlStdOperatorTable.instance(),
                        catalogReader,
                        defaultTypeFactory,
                        SqlValidator.Config.DEFAULT);
        SqlNode validatedSqlNode = validator.validate(originalSqlNode);

        // Step 3. Convert AST to Rel
        RelOptCluster relOptCluster =
                RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(defaultTypeFactory));

        SqlToRelConverter sqlToRelConverter =
                new SqlToRelConverter(
                        // no view expansion
                        (type, query, schema, path) -> null,
                        validator,
                        catalogReader,
                        relOptCluster,
                        // standard expression noralization (see StandardConvertletTable.INSTANCE)
                        StandardConvertletTable.INSTANCE,
                        // default configuration (SqlToRelConverter.config())
                        SqlToRelConverter.config());

        RelRoot relRoot = sqlToRelConverter.convertQuery(validatedSqlNode, false, true);
        RelNode originalRelNode = relRoot.rel;

        // Step 4.
        HepProgram hepProgram =
                new HepProgramBuilder()
                        .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                        .addRuleInstance(FILTER_TO_CALC)
                        .build();

        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(originalRelNode);
        RelNode optimizedRelNode = hepPlanner.findBestExp();
        System.out.println(RelOptUtil.toString(originalRelNode));
    }
}
