package com.mvn.spark;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class NetcatDirect {
    public static void main(String[] args) throws InterruptedException {

        if (args.length < 2) {
            System.err.println("Usage: spark-submit --class com.sym.example.DirectNetcatWC <jar_path> host port ");
            System.exit(1);
        }

        SparkConf conf = new SparkConf().setAppName("NetcatDirect");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(args[0], Integer.parseInt(args[1]));

        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();

        jssc.start();              // Start the computation
        jssc.awaitTermination();
    }
}
