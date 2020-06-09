package sparkdemo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Array;
import scala.Tuple2;

public class SparkDemo {

	public static void main(String[] args) {
		// Karol Dzialowski
		// 39259
		
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
		conf.set("spark.driver.bindAddress", "127.0.0.1");
		JavaSparkContext sc = new JavaSparkContext(conf);
//		JavaRDD<String> textFile = sc.textFile("file:///home/maria_dev/shakespeare.txt"); // ścieżka bezwzględna
		JavaRDD<String> textFile = sc.textFile("ml-latest-small/ratings.csv"); // ścieżka względna
		JavaRDD<String[]> wiersze = textFile.map(linia -> linia.split(","));
		JavaPairRDD<String, Integer> idZJedynka = wiersze.mapToPair(wiersz -> new Tuple2<>(wiersz[1], 1));
		JavaPairRDD<String, Integer> filmy = idZJedynka.reduceByKey((a, b) -> a + b);
		JavaPairRDD<Integer, String> filmyOdwrocone = filmy.mapToPair(film -> new Tuple2<>(film._2, film._1));
		JavaPairRDD<Integer, String> ranking = filmyOdwrocone.sortByKey(false);
		ranking.foreach(p -> System.out.println(p));
		sc.stop();
		sc.close();
	}
}
