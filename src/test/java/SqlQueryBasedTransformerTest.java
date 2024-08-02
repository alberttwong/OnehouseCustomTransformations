
import ai.onehouse.transformer.SqlQueryBasedTransformer;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;

/**
 * Unit test for simple App.
 */
class SqlQueryBasedTransformerTest {

    private static SparkSession spark;
    private static JavaSparkContext jsc;
    private static Dataset<Row> inputDF;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder().appName("SqlQueryBasedTransformerTest").master("local").getOrCreate();
        jsc = new JavaSparkContext(spark.sparkContext());
    }

    @Test
    public void testApply() {


        TypedProperties properties = new TypedProperties();
        properties.put("sql", "select * from alberttable;");

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "name1", "{\"kind\": \"Ingress\", \"spec\": {\"rules\": [{\"http\": {\"paths\": [1, 2]}}]}}"),
                RowFactory.create(2, "name2", "{\"kind\": \"Service\", \"spec\": {\"rules\": [{\"http\": {\"paths\": [2, 3]}}]}}")
                );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("json", DataTypes.StringType, false, Metadata.empty())
        });

        inputDF = spark.createDataFrame(data, schema);

        SqlQueryBasedTransformer transformer = new SqlQueryBasedTransformer();
//        Dataset<Row> outputDF = transformer.apply(jsc, spark, inputDF, properties);

/*         // construct the expected dataframe
        List<Row> expectedData = Arrays.asList(
                RowFactory.create(1, "name1", "{\"kind\": \"Ingress\", \"spec\": {\"rules\": [{\"http\": {\"paths\": [1, 2]}}]}}", "1"),
                RowFactory.create(2, "name2", "{\"kind\": \"Service\", \"spec\": {\"rules\": [{\"http\": {\"paths\": [2, 3]}}]}}", "2")
        );
        StructType expectedSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("json", DataTypes.StringType, false, Metadata.empty()),
                new StructField("flattened", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> expectedDF = spark.createDataFrame(expectedData, expectedSchema);
 */
        //Assertions.assertEquals(expectedDF.collectAsList(), outputDF.collectAsList());
    }
    
}
