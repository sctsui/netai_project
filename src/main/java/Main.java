import kafka.Kafka;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import static spark.Spark.*;
import org.apache.spark.streaming.kafka.*;

import java.util.*;

public class Main {
    private static final int PORT = 8080;
    public static void main(String[] args) throws InterruptedException {
        /*port(PORT);
        get("/hello", (req, res) -> "Hello World");*/

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaConsumer");
        String topics = "test";
        String brokers = "localhost:9092";
        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(1));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
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

        directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}