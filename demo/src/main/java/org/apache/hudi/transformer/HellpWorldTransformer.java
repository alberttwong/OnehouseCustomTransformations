package org.apache.hudi.transformer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class HellpWorldTransformer implements Transformer {

 private static final Logger LOG = LogManager.getLogger(HellpWorldTransformer.class);

 @Override
 public Dataset<Row> apply(JavaSparkContext javaSparkContext, SparkSession sparkSession, Dataset<Row> dataset, TypedProperties typedProperties) {
   LOG.info("Within hello world transformer");
   return dataset.withColumn("new_col", functions.lit("abc"));
 }
}
