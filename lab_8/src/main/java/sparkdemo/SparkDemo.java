package sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.col;

public class SparkDemo {
    public static void main(String[] args) {
        // Ustawienie Sparka
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Word Count");
        conf.set("spark.driver.bindAddress", "127.0.0.1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Wczytanie danych z xml
        Dataset<Row> pages = spark.read().format("xml").option("rowTag", "page").load("wikipedia/export.xml");

        pages.printSchema();

        // Stworzenie kolekcji powiązań każdego artykułu
        Dataset edges = pages.select(col("title"), col("revision.text._VALUE"))
                .flatMap((FlatMapFunction<Row, Relation>) page -> {
                    String title = page.getString(0);
                    String text = page.getString(1);
                    Pattern pattern = Pattern.compile("\\[\\[([.[^\\]]]+)\\]\\]");
                    Matcher matcher = pattern.matcher(text);
                    List<Relation> relations = new ArrayList<>();
                    while (matcher.find()) {
                        String link = matcher.group(1);
                        relations.add(new Relation(title, link));
                    }
                    return relations.iterator();
                }, Encoders.bean(Relation.class));
        edges.show(Integer.MAX_VALUE, false);

        // Stworzenie grafu na podstawie kolekcji reprezentującej krawędzie grafu
        GraphFrame graphFrame = GraphFrame.fromEdges(edges);
        graphFrame.vertices().show();
        graphFrame.edges().show();

        // Obliczenie PageRank dla 3 iteracji
        graphFrame.pageRank().maxIter(3).resetProbability(0.15).run().vertices().show();

        sc.stop();
        sc.close();
    }

    public static class Relation {
        String src;
        String dst;

        public Relation(String src, String dst) {
            this.src = src;
            this.dst = dst;
        }

        public String getSrc() {
            return src;
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public String getDst() {
            return dst;
        }

        public void setDst(String dst) {
            this.dst = dst;
        }
    }
}
