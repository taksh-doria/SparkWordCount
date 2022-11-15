package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class Main {
    public static void main(String[] args)
    {
        System.out.println("Hello world!");
        SparkConf conf=new SparkConf().setMaster("local").setAppName("News Word Count");
        JavaSparkContext context=new JavaSparkContext(conf);
        JavaRDD<String> inputfile=context.textFile("Canada.json");
        JavaPairRDD countdata=inputfile.flatMap((x->Arrays.asList(x.split(" ")).iterator())).mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y).sortByKey();
        countdata.saveAsTextFile("output.txt");

    }
}