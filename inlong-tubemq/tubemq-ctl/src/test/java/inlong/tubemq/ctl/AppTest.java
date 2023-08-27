package inlong.tubemq.ctl;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AppTest {

    public App app = new App();

    @Test
    public void testTopicCreate() {
        String[] args = {"topic", "create", "-brokerId", "1", "-topicName", "ts", "-confModAuthToken", "abc"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testTopicDelete() {
        String[] args = {"topic", "delete", "-brokerId", "1", "-topicName", "ts", "-confModAuthToken", "abc"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testTopicUpdate() {
        String[] args = {"topic", "update", "-brokerId", "1", "-topicName", "ts", "-confModAuthToken", "abc", "-acceptPublish", "false", "-acceptSubscribe", "false"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testTopicGet() {
        String[] args = {"topic", "get", "-brokerId", "1", "-topicName", "ts"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testTopicList() {
        String[] args = {"topic", "list"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testMsgProduce() {
        String[] args = {"msg", "produce", "-master", "127.0.0.1:8715", "-topic", "demo", "-msg", "ts"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testMsgConsume() {
        String[] args = {"msg", "consume", "-master", "127.0.0.1:8715", "-topic", "demo", "-consumeGroup", "con"};
        int result = app.run(args);
        assertEquals(0, result);
    }

    @Test
    public void testHelp() {
        String[] args = {"-help"};
        int result = app.run(args);
        assertEquals(0, result);
    }
}
