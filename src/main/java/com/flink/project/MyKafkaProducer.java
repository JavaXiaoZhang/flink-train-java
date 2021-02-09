package com.flink.project;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class MyKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.33.10:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        String topic = "zzqq";

        // 死循环 发送kafka数据
        while (true) {
            StringBuilder builder = new StringBuilder();
            builder.append("zzqq").append("\t")
                    .append("CN").append("\t")
                    .append(getLevels()).append("\t")
                    .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\t")
                    .append(getIps()).append("\t")
                    .append(getDomains()).append("\t")
                    .append(getTraffic()).append("\t");

            System.out.println(builder.toString());
            //producer.send(new ProducerRecord(topic, builder.toString()));

            Thread.sleep(2000);
        }
    }

    /**
     * 流量
     * @return
     */
    private static Long getTraffic() {
        return RandomUtils.nextLong(0, 100000);
    }

    private static String getDomains() {
        String[] domains = {"v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"};
        return domains[RandomUtils.nextInt(0, domains.length)];
    }

    /**
     * 获取ip地址
     *
     * @return
     */
    private static String getIps() {
        String[] ips = {"223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"};
        return ips[RandomUtils.nextInt(0, ips.length)];
    }

    // 生成level数据
    public static String getLevels() {
        String[] levels = {"M", "E"};
        return levels[RandomUtils.nextInt(0, levels.length)];
    }
}
