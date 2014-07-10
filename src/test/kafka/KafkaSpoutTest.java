package kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;
import storm.kafka.trident.GlobalPartitionInformation;

import java.util.List;

/**
 * Copyright (c) 2014, Sohu.com All Rights Reserved.
 * <p/>
 * User: jeffreywu  MailTo:jeffreywu@sohu-inc.com
 * Date: 14-7-8
 * Time: PM7:05
 */
public class KafkaSpoutTest {

    @Test
    public void kafkaSpoutUnitTest() {
        GlobalPartitionInformation info = new GlobalPartitionInformation();
        info.addPartition(0, new Broker("10.10.52.146", 9092));


        BrokerHosts brokerHosts = new StaticHosts(info);
        // TODO
        // BrokerHosts brokerHosts = new ZkHosts("192.168.193.4:2181");
        String topic = "test";
        String zkRoot = "/fkastorm";
        String spoutId = "spout4";

        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot,
                spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new TestMessageScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        kafkaSpout.nextTuple();
    }

    public static class TestMessageScheme implements Scheme {
        private static final Logger LOGGER = LoggerFactory
                .getLogger(TestMessageScheme.class);

        @Override
        public List<Object> deserialize(byte[] ser) {
            try {
                String msg = new String(ser, "UTF-8");
                return new Values(msg);
            } catch (Exception e) {
                LOGGER.error("Cannot parse the provided message!");
            }
            return null;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("msg");
        }

    }

}
