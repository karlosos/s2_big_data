package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.Properties;

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

        // 1 Ładowanie obiektów Data Frame z plików tekstowych
        Dataset<Row> moviesJSON = spark.read().json("movies/movies.json");
        Dataset<Row> moviesCSV = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("movies/movies.csv");

        //moviesJSON.show();
        //moviesJSON.printSchema();

        //moviesCSV.show();
        //moviesCSV.printSchema();

        // 2 Zadawanie zapytań w języku SQL
        moviesJSON.createOrReplaceTempView("moviesJSON");
        moviesCSV.createOrReplaceTempView("moviesCSV");
        Dataset<Row> sqlMoviesCSV = spark.sql("SELECT * FROM moviesCSV WHERE movieId > 500 ORDER BY movieId DESC");
        Dataset<Row> sqlRatingsJSON = spark.sql("SELECT * FROM moviesJSON WHERE movieId > 500 ORDER BY movieId DESC");
        //sqlMoviesCSV.show();
        //sqlRatingsJSON.show();

        // 3 Operacje łączenia i agregacji
        Dataset<Row> ratingsCSV = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("movies/ratings.csv");
        ratingsCSV.createOrReplaceTempView("ratingsCSV");
        Dataset<Row> sqlRatingsCSV = spark.sql("SELECT COUNT(movieId) FROM ratingsCSV GROUP BY movieId");
        //sqlRatingsCSV.show();

        Dataset<Row> sqlMR = sqlRatingsCSV.crossJoin(sqlMoviesCSV);
        //sqlMR.sort("count(movieId)").show();

        // 4 Tworzenie obiektów typu DataSet
        Encoder<Movie> MovieEncoder = Encoders.bean(Movie.class);
        Dataset<Movie> movies = spark.read().json("movies/movies.json").as(MovieEncoder);
        //movies.show();

        // 5 Wczytanie danych o filmach i ocenach z serwera MySql
        Properties p = new Properties();
        p.setProperty("user", "kukuruku_bigdata");
        p.setProperty("password", "Bigdata123!");
        p.setProperty("serverTimezone", "UTC");
        Dataset<Row> moviesJDBC = spark.read().jdbc("jdbc:mysql://kukuruku.linuxpl.info/kukuruku_bigdata", "movies", p);
        moviesJDBC.show();
        Dataset<Row> ratingsJDBC = spark.read().jdbc("jdbc:mysql://kukuruku.linuxpl.info/kukuruku_bigdata", "ratings", p);
        ratingsJDBC.show();

        sc.stop();
        sc.close();
    }

    public static class Movie {
        public int movieId;
        public String title;
        public String genres;
    }
}
