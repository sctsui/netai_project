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
import java.util.function.Function;

import org.apache.spark.streaming.api.java.*;

public class Main {
    private static final int PORT = 8080;

    public static void main(String[] args) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaConsumer");
        String packet_topics = "test";
        String brokers = "localhost:9092";
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(packet_topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

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
//        //directKafkaStream.print();


        JavaRDD<String> IDSDataSet = streamingContext.sparkContext().textFile("file:///home/swaroop/Kafka_Tar/MyProject/netai_project/IDSLogs/1.txt", 1);

        JavaPairRDD<String, String> IDSKeyValuePair = IDSDataSet.mapToPair(k -> {
            String[] a = k.split(",");
            return new Tuple2<>(a[0] + "," + a[1] + "," + a[2] + "," + a[3] + "," + a[4], a[5]);
        });

//        IDSKeyValuePair.foreach(data -> {
//            System.out.println("key="+data._1() + " value=" + data._2());
//        });

        directKafkaStream.foreachRDD(rdd->{
            JavaPairRDD<String, Tuple2<String, Optional<String>>> out = rdd.leftOuterJoin(IDSKeyValuePair);
            out.foreach(data -> {
                System.out.println("final_key="+data._1() + " final_value=" + data._2());
            });
        });




        //directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}