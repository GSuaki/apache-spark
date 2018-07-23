package kafka;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.Supplier;

public class MainClass {

  static final int KAFKA_PORT = 9092;

  public static void main(String... args) {

    final SparkSession session = SparkSession.builder()
        .appName("BusProcessor")
        .master("local")
        .getOrCreate();

    final Dataset<Row> bandas = session.sqlContext()
        .read()
        .format("jdbc")
        .option("url", args[0])
        .option("dbtable", "banda")
        .option("driver", "com.mysql.jdbc.Driver")
        .load();

    final Dataset<Row> cidades = session.sqlContext()
        .read()
        .format("jdbc")
        .option("url", args[0])
        .option("dbtable", "cidade")
        .option("driver", "com.mysql.jdbc.Driver")
        .load()
        .withColumnRenamed("nome", "cidade_nome");

    bandas.printSchema();

    bandas
        .join(cidades, bandas.col("cidade_id").equalTo(cidades.col("id")), "left_outer")
        .filter(bandas.col("nome").like("%banda%"))
        .select(bandas.col("nome").as("banda"), cidades.col("cidade_nome").as("cidade"))
        .distinct()
        .show();

    session.close();
  }

  static void countFileLines() {

    final JavaSparkContext ctx = new JavaSparkContext(new SparkConf()
        .setMaster("local")
        .setAppName("BusProcessor"));

    final JavaRDD<String> lines = safeExec(ctx, () -> ctx.textFile("../../../Downloads/bus.log"));

    System.out.println(safeExec(ctx, lines::count));

    ctx.close();
  }

  static <T> T safeExec(final JavaSparkContext ctx, final Supplier<T> run) {
    try {
      return run.get();
    } catch (Throwable t) {
      ctx.close();
      throw t;
    }
  }
}
