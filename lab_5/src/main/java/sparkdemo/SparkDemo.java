
package sparkdemo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.codehaus.janino.Java;
import scala.Int;
import scala.Tuple2;
import org.apache.spark.rdd.RDD;

public class SparkDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("ml-latest-small/ratings.csv");
        JavaRDD<Rating> tr = textFile.map(line -> {
            String[] arr = line.split(",");
            return new Rating(
                    Integer.parseInt(arr[0]),
                    Integer.parseInt(arr[1]),
                    Double.parseDouble(arr[2])
            );
        });
        MatrixFactorizationModel model = ALS.train(tr.rdd(), 10, 10);
        Rating[] rec = model.recommendUsers(1, 3);

        //        for (Rating r : rec) {
        //            System.out.println(r);
        //        }

        // Ile każdy film był oceniony
        JavaRDD<String[]> wierszeRatings = textFile.map(linia -> linia.split(","));

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

        // Kazdemu filmowi 3 rekomendacje userow
        RDD<Tuple2<Object, Rating[]>> rekomendacjeUserow = model.recommendUsersForProducts(3);
        JavaPairRDD<Object, Rating[]> rekomendacjeTmp = JavaPairRDD.fromJavaRDD(rekomendacjeUserow.toJavaRDD());
        // Rzutowanie Object na String
        JavaPairRDD<String, Rating[]> rekomendacje = rekomendacjeTmp.mapToPair(el -> new Tuple2<>(el._1.toString(), el._2));

        //        rekomendacje.foreach(recommendation -> {
        //            System.out.println("Movie: " + recommendation._1);
        //            for (Rating r : recommendation._2) {
        //                System.out.println(r.user());
        //            }
        //        });

        // Połączenie rekomendacji z licznikiem filmów
        JavaPairRDD<String, Tuple2<Rating[], Integer>> rekomendacjeZLicznikiem = rekomendacje.join(liczbaOcen);
        //rekomendacjeZLicznikiem.foreach(p -> System.out.println(p._1 + " " + p._2._1 + " " + p._2._2));

        // Filtrowanie rekomendacji wg kryterium min. liczby ocen (np. przynajmniej 10)
        JavaPairRDD<String, Rating[]> rekomendacjeFilterd = rekomendacjeZLicznikiem
                .filter(el -> el._2._2 > 10)
                .mapToPair(el -> new Tuple2<>(el._1, el._2._1));
        rekomendacjeFilterd.foreach(p -> System.out.println(p._1 + " " + p._2));

        // Połączenie rekomendacji z tytułami filmów
        JavaRDD<String> moviesFile = sc.textFile("ml-latest-small/movies.csv"); // ścieżka względna
        JavaRDD<String[]> wierszeMovies = moviesFile.map(linia -> linia.split("((?=.*\\\")(,\\\"|\\\",)|(?!.*\\\"),)"));
        JavaPairRDD<String, String> tytuly = wierszeMovies.mapToPair(line -> new Tuple2(line[0], line[1]));
        JavaPairRDD<String, Rating[]> rekomendacjeTytuly = tytuly.join(rekomendacjeFilterd)
                .mapToPair(el -> el._2);

        rekomendacjeTytuly.foreach(recommendation -> {
            System.out.println("Movie: " + recommendation._1);
            for (Rating r : recommendation._2) {
                System.out.println(r.user());
            }
        });

        sc.stop();
        sc.close();
    }
}
