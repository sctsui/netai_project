import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class Main {
    private static final int PORT = 8080;

    public static void main(String[] args) throws InterruptedException, IOException {
        FileWriter myWriter = new FileWriter("labeledOutput.txt");

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

        // Join key=5tuples, val=payload/alert by 5tuple key
        // out (labeled KeyValue): key=common 5tuple, Tuple2 value = <payload, alert>
        JavaPairRDD<String, String> finalIDSKeyValuePair = IDSKeyValuePair;
        directKafkaStream.foreachRDD(rdd->{
            JavaPairRDD<String, Tuple2<String, Optional<String>>> out = rdd.leftOuterJoin(finalIDSKeyValuePair);

            //todo: debug - writing to file
            //out.saveAsTextFile("file:///home/user/spark/join.txt");
            out.foreach(data -> {
                String final_value = data._2._2.isPresent()? data._2._2.get() : "Not Alerted";
                System.out.println("final_key="+data._1() + " final_value=" + final_value);
                writeToFile(myWriter, "final_key=" + data._1() + " final_value=" + final_value);
            });
        });
        myWriter.close();
        //directKafkaStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();

    }

    private static void writeToFile(FileWriter fw, String contents){
        try {
            fw.write(String.format("%s%n",contents));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}