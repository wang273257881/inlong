package inlong.tubemq.ctl;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.BufferedHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class App {
    // @Option(name = "-r", required = true, usage="Required")
    // public String required;
    
    @Argument(index = 0, usage = "topic or msg", metaVar = "Type")
    private String type;

    @Argument(index = 1, usage = "get/create/update/delete/list for topic, produce/consume for msg", metaVar = "Action")
    private String action;

    @Option(name = "-topicName", usage="TopicName")
    public String topicName;

    @Option(name = "-brokerId", usage="BrokerId")
    public String brokerId;

    @Option(name = "-confModAuthToken", usage="confModAuthToken")
    public String confModAuthToken;

    @Option(name = "-acceptPublish", usage="Does the topic receive publishing requests")
    public String acceptPublish = "true";
    
    @Option(name = "-acceptSubscribe", usage="Does the topic receive subscription requests")
    public String acceptSubscribe = "true";

    @Option(name = "-unflushDataHold", usage="Default maximum allowed size of data to be refreshed")
    public Integer unflushDataHold = 0;
    
    @Option(name = "-numTopicStores", usage="Number of Topic data blocks and partition management groups allowed to be established")
    public Integer numTopicStores = 1;
    
    @Option(name = "-memCacheMsgCntInK", usage="Default maximum memory cache packet size")
    public Integer memCacheMsgCntInK = 10;

    @Option(name = "-memCacheMsgSizeInMB", usage="The total size of the default memory cache package")
    public Integer memCacheMsgSizeInMB = 2;

    @Option(name = "-memCacheFlushIntvl", usage="Maximum allowed waiting refresh interval for memory cache")
    public Integer memCacheFlushIntvl = 20000;

    @Option(name = "-maxMsgSizeInMB", usage="Maximum message packet length setting")
    public Integer maxMsgSizeInMB = 1;

    @Option(name = "-deleteWhen", usage="Time to delete topic")
    public String deleteWhen = "";

    @Option(name = "-unflushThreshold", usage="Maximum allowed number of records to be refreshed")
    public Integer unflushThreshold;

    @Option(name = "-numPartitions", usage="Partition size")
    public Integer numPartitions;

    @Option(name = "-deletePolicy", usage="DeletePolicy")
    public String deletePolicy;

    @Option(name = "-unflushInterval", usage="Maximum allowed time interval to be refreshed")
    public Integer unflushInterval;

    @Option(name = "-createUser", usage="User creating topic")
    public String createUser;

    @Option(name = "-modifyUser", usage="User modifying topic")
    public String modifyUser;

    @Option(name = "-topicStatusId", usage="Status of the topic,0: normal, 1: soft deleted, 2: hard deleted")
    public Integer topicStatusId = 0;

    @Option(name = "-master", usage="Ip and port of master node")
    public String master;

    @Option(name = "-topic", usage="Topic to produce or consume")
    public String topic;

    @Option(name = "-msg", usage="Message to produce")
    public String msg;

    @Option(name = "-consumeGroup", usage="ConsumeGroup")
    public String consumeGroup;

    @Option(name = "-help", usage="Help")
    public Boolean help = false;

    private List<String> cmdArgs;

    public static void main(String[] args) {
        System.exit(new App().run(args));
    }

    public int run(String[] args) {
        System.out.println(Arrays.toString(args));
        CmdLineParser p = new CmdLineParser(this);

        cmdArgs = Arrays.asList(args);

        try {
            p.parseArgument(args);

            if(help) {
                p.printUsage(System.out);
            } else {
                run();
            }
            
            return 0;
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            p.printUsage(System.err);
            return 1;
        }
    }

    private void run() {
        try {
            if(type == null || type.isEmpty()) {
                throw new IOException("type -help for help");
            } else if(type.equals("topic")) {
                printResult(dealTopic());
            } else if(type.equals("msg")){
                dealMsg();
            } else {
                System.out.println("Unsupported operation type");
            }
        } catch (Throwable e) {
            System.err.println(e.getMessage());
        }
        
    }

    private void dealMsg() throws Throwable {
        switch(action) {
            case "produce":
                produceMsg();
                break;
            case "consume":
                consumeMsg();
                break;
            default:
                throw new IOException("Unsupported operation");
        }
    }

    private void consumeMsg() throws Throwable {
        if(master == null || master.isEmpty()) {
            throw new IOException("master required");
        }

        if(topic == null || topic.isEmpty()) {
            throw new IOException("topic required");
        }

        if(consumeGroup == null || consumeGroup.isEmpty()) {
            throw new IOException("consumeGroup required");
        }

        MsgManager.consumeMsg(master, topic, consumeGroup);
    }

    private void produceMsg() throws Throwable {
        if(master == null || master.isEmpty()) {
            throw new IOException("master required");
        }

        if(topic == null || topic.isEmpty()) {
            throw new IOException("topic required");
        }

        if(msg == null || msg.isEmpty()) {
            throw new IOException("msg required");
        }

        MsgManager.produceMsg(master, topic, msg);
    }

    private Map<String, Object> dealTopic() throws ClientProtocolException, IOException, URISyntaxException {
        CloseableHttpResponse response;
        
        switch(action) {
            case "create":
                response = createTopic();
                break;
            case "delete":
                response = deleteTopic();
                break;
            case "update":
                response = updateTopic();
                break;
            case "get":
                response = getTopic(false);
                break;
            case "list":
                response = getTopic(true);
                break;
            default:
                throw new IOException("Unsupported operation");
        }

        return parseResponse(response);
    }

    private CloseableHttpResponse deleteTopic() throws URISyntaxException, ClientProtocolException, IOException {
        checkOptions();
        
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String url;
        URIBuilder builder;
        HttpGet httpGet;
        CloseableHttpResponse response;
        Map<String, Object> dataList;

        url = "http://localhost:8080/webapi.htm?type=op_modify&method=admin_delete_topic_info&modifyUser=ctl";
        builder = new URIBuilder(url);

        builder.setParameter("topicName", topicName);
        builder.setParameter("brokerId", brokerId);
        builder.setParameter("confModAuthToken", confModAuthToken);

        httpGet = new HttpGet(builder.build());

        response = httpclient.execute(httpGet);
        dataList = parseResponse(response);

        if((Double) dataList.get("errCode") == 0) {
            // System.out.println(((ArrayList<Object>)dataList.get("data")).toString());
            dataList = (Map<String, Object>)(((ArrayList<Object>)dataList.get("data")).get(0));

            if((Double) dataList.get("errCode") == 200) {
                System.out.println("softly deleted");

                url = "http://localhost:8080/webapi.htm?type=op_modify&method=admin_remove_topic_info&modifyUser=ctl";

                builder = new URIBuilder(url);

                builder.setParameter("topicName", topicName);
                builder.setParameter("brokerId", brokerId);
                builder.setParameter("confModAuthToken", confModAuthToken);

                httpGet = new HttpGet(builder.build());
                response = httpclient.execute(httpGet);

                return response;
            } else {
                throw new IOException(dataList.get("errInfo").toString());
            }
        } else {
            throw new IOException(dataList.get("errMsg").toString());
        }
    }

    private CloseableHttpResponse updateTopic() throws URISyntaxException, ClientProtocolException, IOException {
        checkOptions();

        CloseableHttpClient httpclient = HttpClients.createDefault();
        String url;
        URIBuilder builder;
        HttpGet httpGet;

        url = "http://127.0.0.1:8080/webapi.htm?type=op_modify&method=admin_modify_topic_info&modifyUser=ctl";
        builder = new URIBuilder(url);

        Map<String, Object> optionMap= new HashMap<>();
        optionMap.put("-topicName", topicName);
        optionMap.put("-brokerId", brokerId);
        optionMap.put("-confModAuthToken", confModAuthToken);
        optionMap.put("-acceptPublish", acceptPublish);
        optionMap.put("-acceptSubscribe", acceptSubscribe);
        optionMap.put("-unflushDataHold", unflushDataHold);
        optionMap.put("-numTopicStores", numTopicStores);
        optionMap.put("-memCacheMsgCntInK", memCacheMsgCntInK);
        optionMap.put("-memCacheMsgSizeInMB", memCacheMsgSizeInMB);
        optionMap.put("-memCacheFlushIntvl", memCacheFlushIntvl);
        optionMap.put("-maxMsgSizeInMB", maxMsgSizeInMB);
        optionMap.put("-unflushThreshold", unflushThreshold);
        optionMap.put("-numPartitions", numPartitions);
        optionMap.put("-deletePolicy", deletePolicy);
        optionMap.put("-unflushInterval", unflushInterval);
        optionMap.put("-deleteWhen", deleteWhen);

        Field[] fields = App.class.getDeclaredFields();
        for (Field field : fields) {
            Option option = field.getAnnotation(Option.class);
            if (option != null && cmdArgs.contains(option.name())) {
                String param = option.name();
                builder.setParameter(param.substring(1), optionMap.get(param).toString());
            }
        }
        
        httpGet = new HttpGet(builder.build());
        return httpclient.execute(httpGet);
    }

    private CloseableHttpResponse createTopic() throws URISyntaxException, ClientProtocolException, IOException {
        checkOptions();

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse response;
        String url;
        URIBuilder builder;
        HttpGet httpGet;
        Map<String, Object> dataList;

        url = "http://localhost:8080/webapi.htm?type=op_modify&method=admin_add_new_topic_record&createUser=ctl";
        builder = new URIBuilder(url);
        
        httpGet = new HttpGet(String.format("http://localhost:8080/webapi.htm?type=op_query&method=admin_query_broker_configure&brokerId=%s", brokerId));
        response = httpclient.execute(httpGet);
        dataList = parseResponse(response);

        if((Double) dataList.get("errCode") == 0) {
            // System.out.println(((ArrayList<Object>)dataList.get("data")).toString());
            dataList = (Map<String, Object>)(((ArrayList<Object>)dataList.get("data")).get(0));

            if(unflushThreshold == null) {
                builder.setParameter("unflushThreshold", Integer.valueOf(((Double)dataList.get("unflushThreshold")).intValue()).toString());
            }

            if(numPartitions == null) {
                builder.setParameter("numPartitions", Integer.valueOf(((Double)dataList.get("numPartitions")).intValue()).toString());
            }

            if(unflushInterval == null) {
                builder.setParameter("unflushInterval", Integer.valueOf(((Double)dataList.get("unflushInterval")).intValue()).toString());
            }

            if(deletePolicy == null || deletePolicy.isEmpty()) {
                builder.setParameter("deletePolicy", dataList.get("deletePolicy").toString());
            }
        } else {
            throw new IOException(dataList.get("errMsg").toString());
        }

        builder.setParameter("topicName", topicName);
        builder.setParameter("brokerId", brokerId);
        builder.setParameter("confModAuthToken", confModAuthToken);

        builder.setParameter("acceptPublish", acceptPublish.toString());
        builder.setParameter("acceptSubscribe", acceptSubscribe.toString());
        builder.setParameter("unflushDataHold", unflushDataHold.toString());
        builder.setParameter("numTopicStores", numTopicStores.toString());
        builder.setParameter("memCacheMsgCntInK", memCacheMsgCntInK.toString());
        builder.setParameter("memCacheMsgSizeInMB", memCacheMsgSizeInMB.toString());
        builder.setParameter("memCacheFlushIntvl", memCacheFlushIntvl.toString());
        builder.setParameter("maxMsgSizeInMB", maxMsgSizeInMB.toString());
        builder.setParameter("deleteWhen", deleteWhen.toString());

        // System.out.println(builder.toString());
        httpGet = new HttpGet(builder.build());
        
        return httpclient.execute(httpGet);
    }
    
    private CloseableHttpResponse getTopic(Boolean isAll) throws URISyntaxException, ClientProtocolException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        String url;
        URIBuilder builder;
        HttpGet httpGet;

        url = "http://localhost:8080/webapi.htm?type=op_query&method=admin_query_topic_info";
        builder = new URIBuilder(url);

        if(!isAll) {
            Map<String, Object> optionMap= new HashMap<>();
            optionMap.put("-topicName", topicName);
            optionMap.put("-brokerId", brokerId);
            optionMap.put("-unflushDataHold", unflushDataHold);
            optionMap.put("-numTopicStores", numTopicStores);
            optionMap.put("-memCacheMsgCntInK", memCacheMsgCntInK);
            optionMap.put("-memCacheMsgSizeInMB", memCacheMsgSizeInMB);
            optionMap.put("-memCacheFlushIntvl", memCacheFlushIntvl);
            optionMap.put("-unflushThreshold", unflushThreshold);
            optionMap.put("-numPartitions", numPartitions);
            optionMap.put("-deletePolicy", deletePolicy);
            optionMap.put("-unflushInterval", unflushInterval);
            optionMap.put("-deleteWhen", deleteWhen);
            optionMap.put("-topicStatusId", topicStatusId);
            optionMap.put("-createUser", createUser);
            optionMap.put("-modifyUser", modifyUser);

            Field[] fields = App.class.getDeclaredFields();
            for (Field field : fields) {
                Option option = field.getAnnotation(Option.class);
                if (option != null && cmdArgs.contains(option.name())) {
                    String param = option.name();
                    builder.setParameter(param.substring(1), optionMap.get(param).toString());
                }
            }
        }

        httpGet = new HttpGet(builder.build());
        return httpclient.execute(httpGet);
    }

    private void checkOptions() throws IOException {
        if(topicName == null || topicName.isEmpty()) {
            throw new IOException("topicName required");
        }

        if(brokerId == null || brokerId.isEmpty()) {
            throw new IOException("brokerId required");
        }

        if(confModAuthToken == null || confModAuthToken.isEmpty()) {
            throw new IOException("confModAuthToken required");
        }

        acceptPublish = acceptPublish.toLowerCase();
        if(!(acceptPublish.equals("true") || acceptPublish.equals("false"))) {
            throw new IOException("acceptPublish should be true or false");
        }

        acceptSubscribe = acceptSubscribe.toLowerCase();
        if(!(acceptSubscribe.equals("true") || acceptSubscribe.equals("false"))) {
            throw new IOException("acceptSubscribe should be true or false");
        }
    }

    private Map<String, Object> parseResponse(CloseableHttpResponse response) throws ParseException, IOException {
        HttpEntity entity = response.getEntity();
        entity = new BufferedHttpEntity(entity);
        String content = EntityUtils.toString(entity);
        
        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> map = gson.fromJson(content, type);

        return map;
    }
    
    private void printResult(Map<String, Object> res) {
        // System.out.println(res.toString());
        Double errCode = (Double) res.get("errCode");
        if(errCode==0) {
            System.out.println(res.get("data").toString());
        } else {
            System.out.format("errCode: %.0f, %s\n", errCode, res.get("errMsg"));
        }
    }

}