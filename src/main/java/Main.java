import kafka.Kafka;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import static spark.Spark.*;
import org.apache.spark.streaming.kafka.*;
import scala.Tuple2;

import java.util.*;

public class Main {
    private static final int PORT = 8080;
    public static void main(String[] args) throws InterruptedException {
        /*port(PORT);
        get("/hello", (req, res) -> "Hello World");*/

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaConsumer");
        String packet_topics = "test";
        String brokers = "localhost:9092";
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(30));
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

        //directKafkaStream.print();
        String ids_topics = "test3";
        Set<String> ids_topicsSet = new HashSet<>(Arrays.asList(ids_topics.split(",")));
        Map<String, String> ids_kafkaParams = new HashMap<>();
        ids_kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, String> ids_directKafkaStream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        String.class,
                        String.class,
                        StringDecoder.class,
                        StringDecoder.class,
                        ids_kafkaParams,
                        ids_topicsSet
                );

        //directKafkaStream.print();
        streamingContext.sparkContext().setLogLevel("ERROR");
        JavaPairDStream<String, Tuple2<String, String>> out_directKafkaStream = directKafkaStream.join(ids_directKafkaStream);
        out_directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}