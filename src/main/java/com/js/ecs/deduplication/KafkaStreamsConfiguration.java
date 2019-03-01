package com.js.ecs.deduplication;



import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfiguration {
	
	@Autowired private KafkaProperties kafkaProperties;
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "RSS-deduplication");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return new StreamsConfig(props);
    }
    final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

    String winDupStateStoreName = "WinDupStore";

    @Bean
    public KStream<JsonNode, JsonNode> kStreamJson(StreamsBuilder builder) {
        /**** Creating a Window  State Store  of winodow size 1 minute (600000 milli seconds) and 5 minutes of retention ***********/

        /*
         * store name
         * retention period  (in millisecs)
         * num of segments
         * window Size (in millisecs)
         * retain duplicates
         */

        WindowBytesStoreSupplier windowStoreSupplier = Stores.persistentWindowStore(winDupStateStoreName, 600000, 2, 600000, false);

        StoreBuilder<WindowStore<String,String>> windowStoreBuilder = Stores.windowStoreBuilder(windowStoreSupplier, Serdes.String(), Serdes.String());

        builder.addStateStore(windowStoreBuilder);

        //System.out.println("step2");

        /**** Finished Creating State Store  ***********/

        KStream<JsonNode, JsonNode> simpleFirstStream =
                builder.stream("Product-location-in", Consumed.with(jsonSerde, jsonSerde))

                        .transformValues(()-> new PaEcsWindowDeduplication(winDupStateStoreName),winDupStateStoreName) // Transformer is called in this line using the class PaEcsWindowDeduplication
                        .filter(TestKafkaStream::filterRecord);// Records are filtered in this line
        simpleFirstStream.to( "Product-location-out",Produced.with(jsonSerde, jsonSerde));
		return simpleFirstStream;
    }


    static class TestKafkaStream {
        static boolean filterRecord(JsonNode k, JsonNode v) {


            if (v != null) {

                //System.out.println("in filter: returning true");
                return true;
            } else {

                //System.out.println("in filter: returning false");
                return false;
            }


        }
    }
    
}

