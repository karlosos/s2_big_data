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
		JavaRDD<String> ratingsFile = sc.textFile("ml-latest-small/ratings.csv"); // ścieżka względna
		JavaRDD<String> moviesFile = sc.textFile("ml-latest-small/movies.csv"); // ścieżka względna
		JavaRDD<String[]> wierszeRatings = ratingsFile.map(linia -> linia.split(","));
		JavaRDD<String[]> wierszeMovies = moviesFile.map(linia -> linia.split(","));

		// Do liczenia sumy ocen
		JavaPairRDD<String, Double> ratingPairs = wierszeRatings.mapToPair(line ->{
			try{
				return new Tuple2(line[1], Double.valueOf(line[2]));
			}catch(Exception e){
				return new Tuple2(line[1], 0.0);
			}
		});
		JavaPairRDD<String, Double> sumaOcen = ratingPairs.reduceByKey((a, b) -> a + b);
		// sumaOcen.foreach(p -> System.out.println(p));

		// Do liczenia liczby ocen
		JavaPairRDD<String, Integer> ratingCountPairs = wierszeRatings.mapToPair(line ->{
			try{
				return new Tuple2(line[1], 1);
			}catch(Exception e){
				return new Tuple2(line[1], 1);
			}
		});
		JavaPairRDD<String, Integer> liczbaOcen = ratingCountPairs.reduceByKey((a, b) -> a + b);
		//liczbaOcen.foreach(p -> System.out.println(p));

		// Obliczenie średniej oceny
		JavaPairRDD<String, scala.Tuple2<Double, Integer>> liczbaOcenSumaOcen = sumaOcen.join(liczbaOcen);
		// liczbaOcenSumaOcen.foreach(p -> System.out.println(p));
		JavaPairRDD<String, Double> sredniaOcen = liczbaOcenSumaOcen.mapToPair(wiersz -> {
			return new Tuple2(wiersz._1, wiersz._2._1/wiersz._2._2);
		});
		//sredniaOcen.foreach(p -> System.out.println(p));

		// Połączenie z nazwami filmów
		JavaPairRDD<String, String> tytuly = wierszeMovies.mapToPair(line ->{
			return new Tuple2(line[0], line[1]);
		});
		JavaPairRDD<Double, String> rankingZNazwami = sredniaOcen.join(tytuly).mapToPair(wiersz -> wiersz._2);
		JavaPairRDD<Double, String> rankingPosortowany = rankingZNazwami.sortByKey(false);
		rankingPosortowany.foreach(p -> System.out.println(p));

		sc.stop();
		sc.close();
	}
}
