package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import java.util.Arrays;
import java.util.List;

public class MapReduceSpark {
    //a)
    public static void wordCount(JavaSparkContext sc) {
        System.out.println("Word Count with Spark");
        JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");
        JavaRDD<String> words = lines
                .map(line -> line.replaceAll("[,\\.\\?!;]", " "))
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(w -> !w.isBlank());

        JavaPairRDD<String, Integer> counts = words
                .mapToPair(w -> new Tuple2<>(w, 1)) 
                .reduceByKey(Integer::sum);

        List<Tuple2<Integer, String>> top10 = counts
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .take(10);
        System.out.println("The Top 10 most frequent words are:");

        for (Tuple2<Integer, String> e : top10) {
            System.out.println(e._2 + " -> " + e._1);
        }
       try (BufferedWriter writer =
            new BufferedWriter(new FileWriter("src/main/resources/output_a.txt"))) {
        writer.write("Results for 4.1 a): ");
        writer.newLine();
        for (Tuple2<Integer, String> e : top10) {
            writer.write(e._2 + " -> " + e._1);
            writer.newLine();
    }  
        } catch (IOException e) {
        e.printStackTrace(); 
    }
    }
    //b)
    public static void longestWords(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");
        JavaRDD<String> words = lines
                .map(line -> line.replaceAll("[,\\.\\?!;]", " "))
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(w -> !w.isBlank());
        JavaPairRDD<Integer, String> lengthWords = words
                .mapToPair(w -> new Tuple2<>(w.length(), w));
        List<Tuple2<Integer, String>> top10Longest = lengthWords
                .sortByKey(false)
                .take(10);
        System.out.println("The Top 10 longest words are:");
        for (Tuple2<Integer, String> e : top10Longest) {
            System.out.println(e._2 + " -> " + e._1);
        }
        try (BufferedWriter writer =
            new BufferedWriter(new FileWriter("src/main/resources/output_b.txt"))) {
            writer.write("Results for 4.1 b): ");
            writer.newLine();
            for (Tuple2<Integer, String> e : top10Longest) {
                writer.write(e._2 + " -> " + e._1);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //c)
    public static void averageWordLength(JavaSparkContext sc) {
        JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");
        JavaRDD<String> words = lines
                .map(line -> line.replaceAll("[,\\.\\?!;]", " "))
                .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
                .filter(w -> !w.isBlank());
        long totalWords = words.count();
        JavaPairRDD<Integer, String> lengthWords = words
                .mapToPair(w -> new Tuple2<>(w.length(), w));
        long totalLength = lengthWords
                .keys()
                .reduce(Integer::sum);
        double averageLength = (double) totalLength / totalWords;
        System.out.printf("The average word length is: %.2f%n", averageLength);
        try (BufferedWriter writer =
            new BufferedWriter(new FileWriter("src/main/resources/output_c.txt"))) {
            writer.write("Results for 4.1 c): ");
            writer.newLine();
            writer.write(String.format("The average word length is: %.2f%n", averageLength));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //d)
public static void anagramm(JavaSparkContext sc) {
    System.out.println("Anagram groups:");

    JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");

    JavaRDD<String> words = lines
            .map(line -> line.replaceAll("[,\\.\\?!;]", " "))
            .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
            .filter(w -> !w.isBlank())
            .map(String::toLowerCase)
            .distinct();

    JavaPairRDD<String, String> keyedByAnagram = words
            .mapToPair(word -> {
                char[] chars = word.toCharArray();
                Arrays.sort(chars);
                return new Tuple2<>(new String(chars), word);
            });

    JavaPairRDD<String, Iterable<String>> grouped =
            keyedByAnagram.groupByKey();

    grouped
            .filter(t -> {
                int count = 0;
                for (String ignored : t._2) count++;
                return count > 1;
            })
            .foreach(t -> {
                System.out.print("Anagram group: ");
                for (String w : t._2) {
                    System.out.print(w + " ");
                }
                System.out.println();
            });

    System.out.println("Anagram processing completed.");
}

// e)
public static void consecutiveWords(JavaSparkContext sc) {
    JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");
    JavaRDD<String> words = lines
            .map(line -> line.replaceAll("[,\\.\\?!;]", " "))
            .flatMap(line -> Arrays.asList(line.split("\\s+")).iterator())
            .filter(w -> !w.isBlank())
            .map(String::toLowerCase);

    JavaPairRDD<Long, String> indexed =
            words.zipWithIndex().mapToPair(t -> new Tuple2<>(t._2, t._1));

    JavaPairRDD<Long, Tuple2<String, String>> joined =
            indexed.join(indexed.mapToPair(t -> new Tuple2<>(t._1 - 1, t._2)));

    JavaPairRDD<String, Integer> bigrams = joined
            .mapToPair(t -> new Tuple2<>(
                    t._2._2 + " " + t._2._1, // previous + current
                    1
            ))
            .reduceByKey(Integer::sum);

    System.out.println("Bigrams occurring more than 10 times:");

    bigrams
            .filter(t -> t._2 > 10)
            .foreach(t ->
                    System.out.println(t._1 + " -> " + t._2)
            );
}






    //Run Methods
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapReduceSpark")
                .setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            wordCount(sc);
            longestWords(sc);
            averageWordLength(sc);
            anagramm(sc);
            consecutiveWords(sc);
        }

    }

}
