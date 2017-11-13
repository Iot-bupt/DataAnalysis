package com.tjlcast.demo;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Created by tangjialiang on 2017/11/13.
 * git: mine and bupt
 *
 */
public class KafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.108.219.61:9092");
        props.put("metadata.broker.list","10.108.219.61:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer(props);
        // 发送业务消息
        // 读取文件 读取内存数据库 读socket端口
        int i = 0 ;
        while (true) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 温度随机数

            String info = generateAnItem() ;
            ProducerRecord<String, String> mesg = new ProducerRecord<String, String>("device",
                    info) ;

            System.out.format("topic: %s info: %s\n", "test", mesg.value()) ;
            producer.send(mesg);

            i++ ;
        }
    }

    public static String generateAnItem() {
        // {"uid":"922291","data":10,"current_time":"2017-11-06 17:44:45"}
        String[] uids = {"922291", "922292", "922293", "922294", "922295"} ;
        int[] datas = {1, 2, 3, 4, 5} ;

        String uid = uids[getRandom()] ;
        int data = datas[getRandom()] ;
        String date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        JSONObject jsonObject = new JSONObject() ;
        jsonObject.put("uid", uid) ;
        jsonObject.put("data", data) ;
        jsonObject.put("current_time", date) ;

        String jsonStr = jsonObject.toJSONString() ;
        return jsonStr ;
    }

    public static int getRandom() {
        int max=5;
        int min=1;
        Random random = new Random();

        int s = random.nextInt(max)%(max-min+1) + min;
        return s-1 ;
    }
}
