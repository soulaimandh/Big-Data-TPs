package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Exercice_CountventParVille_telnet {
    public static void main(String[] args) throws InterruptedException {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf();
        conf.setAppName("tp stream spark").setMaster("local[*]");
        JavaStreamingContext sc = new JavaStreamingContext(conf,new Duration(5000));
        JavaReceiverInputDStream<String> dStreamLine = sc.socketTextStream("localhost",9090);
        JavaDStream<String> dStreamword = dStreamLine.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Double> dStreamwordPair = dStreamword.mapToPair((vent)->new Tuple2<>(vent.split(" ")[1],
                Double.parseDouble(vent.split(" ")[3])));
        JavaPairDStream<String, Double> dStreamwordcount = dStreamwordPair.reduceByKey((a,b)->a+b);
        dStreamwordcount.print();
        sc.start();
        sc.awaitTermination();

    }
}