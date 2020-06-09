
package sparkdemo;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Int;
import scala.Tuple2;

public class SparkDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> moviesFile = sc.textFile("ml-latest-small/movies.csv"); // ścieżka względna

        // 1. policzy i wyświetli informacje o liczbie przypisanych gatunków filmowych do każdego filmu (na podstawie pliku movies.csv)
        JavaRDD<String[]> wierszeMovies = moviesFile.map(linia -> linia.split("((?=.*\\\")(,\\\"|\\\",)|(?!.*\\\"),)"));
        JavaPairRDD<String, String[]> genresMovies = wierszeMovies.mapToPair(line -> new Tuple2(line[0], line[2].split("\\|")));
        JavaPairRDD<String, Integer> genresMoviesCount = genresMovies.mapToPair((line -> new Tuple2<>(line._1, line._2.length)));
        //genresMoviesCount.foreach(p -> System.out.println(p));

        // 2. na podstawie kolumny z danymi o gatunkach filmowych w pliku z filmami utworzy listę wszystkich gatunków
        // (bez powtórzeń) i wyświetli ją posortowaną wg popularności (liczby filmów przypisanych do kategorii)
        JavaRDD<String> genres = genresMovies.flatMap(s -> Arrays.asList(s._2).iterator());
        JavaPairRDD<String, Integer> genresCount = genres.mapToPair(genre -> new Tuple2<>(genre, 1));
        JavaPairRDD<String, Integer> genresCountReduced = genresCount.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, Integer> genresCountSorted = genresCountReduced.mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1)).sortByKey(false).mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));
        //genresCountSorted.foreach(p -> System.out.println(p));

        // 3. stworzy listę tagów przypisanych przez użytkowników poszczególnym filmom (na podstawie pliku tags.csv).
        // Policzy ile tagów zostało przypisanych do każdego filmu. Wyświetli te dane używając tytułów filmów.
        JavaRDD<String> tagsFile = sc.textFile("ml-latest-small/tags.csv"); // ścieżka względna
        JavaRDD<String[]> wierszeTags = tagsFile.map(linia -> linia.split(","));
        JavaPairRDD<String, Integer> tagsZJedynkami = wierszeTags.mapToPair(linia -> new Tuple2<>(linia[1], 1));
        JavaPairRDD<String, Integer> tagsCount = tagsZJedynkami.reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, String> tytuly = wierszeMovies.mapToPair(line -> new Tuple2(line[0], line[1]));
        JavaPairRDD<String, Integer> tytulySumaTagow = tytuly.join(tagsCount).mapToPair(el -> el._2);
        // tytulySumaTagow.foreach(p -> System.out.println(p));

        // 4. znajdzie i wyświetli te tytuły filmów, które mają przypisanych więcej tagów niż kategorii.
        JavaPairRDD<String, Tuple2<Integer, Integer>> movieIdAndTagsGenres = tagsCount.join(genresMoviesCount);
        JavaRDD<String> moviesWithMoreTags = tytuly.join(movieIdAndTagsGenres)
                .mapToPair(el -> el._2)
                .filter(line -> line._2._1 > line._2._2)
                .map(line -> line._1);
        moviesWithMoreTags.foreach(p -> System.out.println(p));


        sc.stop();
        sc.close();
    }
}