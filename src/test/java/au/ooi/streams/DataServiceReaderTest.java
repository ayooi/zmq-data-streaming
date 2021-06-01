package au.ooi.streams;

import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import static org.junit.Assert.assertTrue;

public class DataServiceReaderTest {

    @Test
    public void foo() throws InterruptedException {
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

        DataServiceReader reader = new DataServiceReader(serviceName, ctx, locatorUrl);

        ZMQ.Socket socket = ctx.createSocket(SocketType.PUSH);
        socket.bind(dataUrl);
        socket.send("Payload");

        reader.process();

        assertTrue(reader.hasData());
    }
}