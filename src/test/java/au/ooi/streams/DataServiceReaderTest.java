package au.ooi.streams;

import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataServiceReaderTest {

    @Test
    public void testRead() throws InterruptedException {
        String locatorUrl = "inproc://service-locator-url";
        ZContext ctx = new ZContext();
        String serviceName = "service-name";

        ZMQ.Socket router = ctx.createSocket(SocketType.ROUTER);
        router.bind(locatorUrl);
        String dataUrl = "inproc://data-service-url";

        new Thread(() -> {
            while (true) {
                ZMsg msg = ZMsg.recvMsg(router);
                ZMsg result = new ZMsg();
                result.add(msg.pollFirst());
                result.add(dataUrl);
                result.send(router);
            }
        }).start();

        DataServiceReader reader = new DataServiceReader(serviceName, ctx, locatorUrl, Executors.newCachedThreadPool());

        ZMQ.Socket socket = ctx.createSocket(SocketType.PUSH);
        socket.bind(dataUrl);
        socket.send("Payload");

        // call twice because there will have been a IgnoreMessage that came through
        reader.process();
        reader.process();

        assertTrue(reader.hasData());
        assertEquals("Payload", new String(reader.take()));
    }
}