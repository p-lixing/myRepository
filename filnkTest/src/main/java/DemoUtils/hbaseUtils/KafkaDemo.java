package DemoUtils.hbaseUtils;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

public class KafkaDemo {

    public static final String TOPIC = "topic_li_test";
    public  static String  brokerlist="172.16.122.50:9092,172.16.122.55:9092,172.16.122.57:9092";
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("usage: java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/kafka/libs:/usr/hdp/2.2.0.0-2041/hadoop  -cp dev-demo-1.0-SNSHOT.jar com.tencent.tbds.demo.KafkaDemo producer|consumer");
            return;
        } else if (args[0].equals("consumer")) {
            consumerDemo();
        } else if (args[0].equals("producer")) {
            producerDemo();
        } else {
            System.out.println("usage: java -Djava.ext.dirs=/usr/hdp/2.2.0.0-2041/kafka/libs:/usr/hdp/2.2.0.0-2041/hadoop  -cp dev-demo-1.0-SNSHOT.jar com.tencent.tbds.demo.KafkaDemo producer|consumer");
        }
    }

    private static void producerDemo() throws Exception {
        Properties properties = getProducerProperties(brokerlist);
        tbdsAuthentication(properties);
        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        Future<RecordMetadata> future = null;

        Scanner scanner = new Scanner(System.in);
        String input;
        int sendNo = 0;
        while (true) {
            System.out.print("Enter what you want to send(exit for quit):");
            input = scanner.nextLine();
            if (input.equals("exit") || input.equals("quit")) {
                producer.close();
                return;
            }
            future = producer.send(new ProducerRecord<String, String>(TOPIC, String.valueOf(++sendNo), input));
            System.out.println("send message:[topic, offset, key, value]--->[" + TOPIC + "," + future.get().offset() + "," + sendNo + "," + input + "]");
        }
    }
    private static void consumerDemo() throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "li_test_group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //加入tbds认证信息
//        tbdsAuthentication(props);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);
        for (ConsumerRecord<String, String> record : records) {
        System.out.println("Received message: [" + record.key() + ", " + record.value() + "] at offset " + record.offset());
        }
        }
        }

private static Properties getDefaultProperties() throws IOException {
        Properties properties = new Properties();

        // 也可以把认证以及配置信息以key-value的方式写入到一个properties配置文件中，使用时直接载入
//        properties.load(new BufferedInputStream(new FileInputStream("/tmp/li_test/kafka/kafka_sasl.properties")));

        // hard code config information
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "tbds-10-3-0-13:6667,tbds-10-3-0-4:6667,tbds-10-3-0-6:6667");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "TestConsumerGroup");

        //add authentication information directly
//        properties.put("security.protocol", "SASL_TBDS");
//        properties.put("sasl.mechanism", "TBDS");
//        properties.put("sasl.tbds.secure.id", "v25ClSQKQ2s6FJcKObZfg4gmiuYG44Y1Ock3");
//        properties.put("sasl.tbds.secure.key", "ex0F35VdUYCOaZ4r3BblicNOMhqRtlXz");

        System.out.println("\nCheck read properties successfully:[sasl.tbds.secure.key]--->" + properties.getProperty("sasl.tbds.secure.key"));
        return properties;
        }
public static Properties getProducerProperties(String brokerList) {

        //create instance for properties to access producer configs
        Properties props = new Properties();
        /*
         *1.这里指定server的所有节点
         *2. product客户端支持动态broke节点扩展，metadata.max.age.ms是在一段时间后更新metadata。
         */
        props.put("bootstrap.servers", brokerList);
        /*
         * Set acknowledgements for producer requests.
         * acks=0：意思server不会返回任何确认信息，不保证server是否收到，因为没有返回retires重试机制不会起效。
         * acks=1：意思是partition leader已确认写record到日志中，但是不保证record是否被正确复制(建议设置1)。
         * acks=all：意思是leader将等待所有同步复制broker的ack信息后返回。
         */
        props.put("acks", "1");
        /*
         * 1.If the request fails, the producer can automatically retry,
         * 2.请设置大于0，这个重试机制与我们手动发起resend没有什么不同。
         */
        props.put("retries", 3);
        /*
         * 1.Specify buffer size in config
         * 2. 10.0后product完全支持批量发送给broker，不乱你指定不同parititon，product都是批量自动发送指定parition上。
         * 3. 当batch.size达到最大值就会触发dosend机制。
         */
        props.put("batch.size", 16384);
        /*
         * Reduce the no of requests less than 0;意思在指定batch.size数量没有达到情况下，在1s内也回推送数据
         */
        props.put("linger.ms", 1000);
        /*
         * 1. The buffer.memory controls the total amount of memory available to the producer for buffering.
         * 2. 生产者总内存被应用缓存，压缩，及其它运算。
         */
        props.put("buffer.memory", 1638400);//测试的时候设置太大，Callback 返回的可能会不打印。
        /*
         * 可以采用的压缩方式：gzip，snappy
         */
        //props.put("compression.type", gzip);
        /*
         * 1.请保持producer，consumer 序列化方式一样，如果序列化不一样，将报错。
         */
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return props;
        }
public static void tbdsAuthentication(Properties props) {
        props.put("security.protocol", "SASL_TBDS");
        props.put("sasl.mechanism", "TBDS");
        props.put("sasl.tbds.secure.id","SBULhm9xjfX4f6Lx8pNQM5di1Ymf0i0edJeG");
        props.put("sasl.tbds.secure.key","dpVi95pCjR7xBSkeLlSicEublQffFk2Z");
        }
        }
