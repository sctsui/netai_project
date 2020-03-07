import com.google.inject.internal.cglib.core.$DefaultGeneratorStrategy;
import kafka.Kafka;
import kafka.serializer.StringDecoder;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.StreamInputFormat;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import static spark.Spark.*;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.util.*;

import org.apache.spark.streaming.api.java.*;

public class Main {
    private static final int PORT = 8080;

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaConsumer");
        String packet_topics = "TutorialTopic";
        String brokers = "128.111.52.91:9092";
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(packet_topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaRDD<String> IDSDataSet = streamingContext.sparkContext().textFile("file:///home/user/spark/IDSLogs/alert_csv.txt", 1);
        //JavaRDD<String> IDSDataSet = streamingContext.sparkContext().textFile("file:///home/sabrina/IdeaProjects/netai_project/IDSLogs/1.txt", 1);


        JavaPairRDD<String, String> IDSKeyValuePair = IDSDataSet.mapToPair(k -> {
            String[] a = k.split(",");
            if (!a[0].isEmpty() && !a[1].isEmpty() && !a[2].isEmpty() && !a[3].isEmpty() && !a[4].isEmpty()){
                String key = String.join(",",a[0],a[1],a[2],a[3],a[4]).replaceAll("\\s","");
                String value = String.join(",",a[5],a[6],a[7]);
                return new Tuple2<>(key, value);
            }
            return null;
        });

        //remove null items from IDSKeyValuePair
        IDSKeyValuePair = IDSKeyValuePair.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> v1) throws Exception {
                return v1 != null;
            }
        });
        IDSKeyValuePair = IDSKeyValuePair.distinct();

        IDSKeyValuePair.foreach(data -> {
            if (data != null) {
                System.out.println("key=" + data._1() + " value=" + data._2());
            }
        });

        JavaPairInputDStream<String, String> directKafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        kafkaParams,
                        topicsSet
                );

        streamingContext.sparkContext().setLogLevel("ERROR");

        JavaPairRDD<String, String> finalIDSKeyValuePair = IDSKeyValuePair;
        directKafkaStream.foreachRDD(rdd->{
            JavaPairRDD<String, Tuple2<String, Optional<String>>> out = rdd.leftOuterJoin(finalIDSKeyValuePair);
            out.foreach(data -> {
                String final_value = data._2._2.isPresent()? data._2._2.get() : "Not Alerted";
                System.out.println("final_key="+data._1() + " final_value=" + final_value);
            });
        });

        //directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}