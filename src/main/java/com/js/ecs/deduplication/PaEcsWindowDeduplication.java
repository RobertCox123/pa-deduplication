package com.js.ecs.deduplication;

import org.apache.kafka.streams.kstream.ValueTransformer;


import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

/*
 * This class will extends the ValueTransformer  to transform the input JSON Record (coming from Stream) to null if it is already been processed earlier
 * Transform method in this class will check the record's Key in a timed window cache of defined period (configurable). If Key is found in the cache this out record
 * is set to null and returned back. if key is not found in the cache it means the record came here first time, this record will be forwarded to the downstream processes
 * at it is and a entry will also be created in the cache.
 *Product-location-in 2 757 @ 0: {"STORE_ITEM_LOCATION_ID":17338487228,"STORE_LOCATION_ID":8253951,"EVENT_TYPE":"U","STORE_LOC":757,"STORE_ITEM":"7009553",
 * "PRIMARY_LOCATION_IND":1,"CAPACITY":12.0,"WIDTH":3.0,"LAST_UPDATE_USERID":"0268.0757","TRAN_DATE":1548923530000,"TICKET_PROCESSED":"N","ITEM_SEQUENCE":null,
 * "SHORT_SECTION_NAME":null,"MEZZANINE":null,"NEAREST_AISLE":null,"ACTUAL_SEGMENT_NUMBER":null,"ACTUAL_AISLE_ID":null,"ACTUAL_CONSTANT_BAY_ID":null,
 * "ACTUAL_LOGICAL_BAY_ID":null,"ACTUAL_SHELF_ID":null,"ACTUAL_PLINTH_ID":null,"ONLINE_PROCESSED":"N","ITEM_LOC_CHG_TXN_ID":259135299,"CLASS":"297",
 * "DEFAULT_PACK_SIZE":3.0,"STORE_LOCATION_TYPE":0,"STORE_ITEM_LOCATION_SEQ":
 * Kafka stream WindowStore is used as a cache
 */

public class PaEcsWindowDeduplication implements ValueTransformer<JsonNode, JsonNode> {



    private WindowStore<String, String> windowStore;
    private final String storeName;
    private ProcessorContext context;
    private static final Integer BACKINTIME = 10*60*1000;

    public PaEcsWindowDeduplication(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        windowStore = (WindowStore<String,String>) this.context.getStateStore(storeName);
    }

    @Override
    public JsonNode transform(JsonNode value) {

        ObjectMapper om = new ObjectMapper();
        JsonNode rootNode = null;



        try {

            String recordValue = (String) value.toString();

            rootNode = 	om.readTree(recordValue);

            if (!rootNode.isNull()) {
                //System.out.println("in Transformer2: not null");

                // get Transaction id from the record

                JsonNode txnNode = rootNode.path("item_loc_chg_txn_id");
                String txnid = (String)txnNode.asText();



                /****                     Search in window store  in last one minute    *****/

                long currentTime = context.timestamp();
                long from = currentTime - BACKINTIME/2; // setting the window segment starting time
                long to = currentTime + BACKINTIME/2;	 // setting the window segment end time

                long diff = to-from;


                //System.out.println(new Date(from) + ", "+ new Date(to) + ", "  +diff);
                //System.out.println("searching " + txnid + " in last 10 minutes");

                WindowStoreIterator<String> iterator = windowStore.fetch(txnid,from, to);
                //WindowStoreIterator<String> iterator = windowStore.fetch(txnid,from, to);// fetch all records from window store with key in time range from and to
                boolean entryFound = iterator.hasNext(); //  check if a record with exists in window store using iterator

                if(!entryFound) {

                    long eventTime = context.timestamp();
                    windowStore.put(txnid,txnid,eventTime);        // adding a record in the window store - key, Value, eventTime,

                    //System.out.println("in Transformer3: not in state store.adding it now: "+ txnid + " , at "  + eventTime);

                    return rootNode;

                }else { // if found in store, it means duplicate, so set the return record to null which will be filtered in next processing node. Kstream.filter


                    System.out.println("Duplicate transaction id found: " + txnid);

                    rootNode=null;
                    return rootNode;


                }


            }else // if rootNode == Null
            {
                System.out.println("in Transformer2: null");
            }

        } catch(Exception e) {
            e.printStackTrace();
        }


        return rootNode;

    }

    @Override
    @SuppressWarnings("deprecation")
    public JsonNode punctuate(long timestamp) {
       return null;  //no-op null values not forwarded.
   }

    @Override
    public void close() {
        //no-op
    }


}
