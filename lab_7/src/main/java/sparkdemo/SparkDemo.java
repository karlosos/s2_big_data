package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class SparkDemo {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> orders = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("instacart/orders.csv");
        orders.createOrReplaceTempView("orders");

        Dataset<Row> products = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("instacart/products.csv");
        products.createOrReplaceTempView("products");

        Dataset<Row> departments = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("instacart/departments.csv");
        departments.createOrReplaceTempView("departments");

        // Zadanie 1
        Dataset<Row> hours = spark.sql("SELECT order_hour_of_day, COUNT(*) FROM orders GROUP BY order_hour_of_day ORDER BY order_hour_of_day");
        hours.show(24);

        // Zadanie 2
        // zgrupowane po dniach tygodnia
        Dataset<Row> daysofweek = spark.read().json("instacart/daysofweek.json");
        daysofweek.createOrReplaceTempView("dow");

        Dataset<Row> ordersByDay = spark.sql("SELECT name, count(order_dow) FROM dow LEFT JOIN orders ON dow.id=orders.order_dow GROUP BY dow.name ORDER BY count(order_dow)");
        ordersByDay.show();

        // Zadanie 3
        Dataset<Row> depCount = spark.sql("SELECT departments.department, COUNT(products.product_id) FROM departments INNER JOIN products ON products.department_id=departments.department_id GROUP BY departments.department  ORDER BY COUNT(products.product_id) ASC");
        depCount.show(25);

        Dataset<Row> productsPercent = spark.sql("SELECT departments.department, COUNT(products.product_id) AS suma, (COUNT(products.product_id) * 100 / (Select Count(*) From products)) AS procent " +
                "FROM departments " +
                "INNER JOIN products ON products.department_id=departments.department_id " +
                "GROUP BY departments.department " +
                "ORDER BY procent ASC");
        productsPercent.show(21);

        sc.stop();
        sc.close();
    }
}
