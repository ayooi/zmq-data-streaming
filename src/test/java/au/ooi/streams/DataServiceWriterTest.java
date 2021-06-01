package au.ooi.streams;

import org.junit.Assert;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class DataServiceWriterTest {

    @Test
    public void foo() {
        ZContext ctx = new ZContext();

        ZMQ.Socket router = ctx.createSocket(SocketType.ROUTER);
        String locatorUrl = "inproc://service-location";
        router.bind(locatorUrl);
        String dataUrl = "inproc://random-location";
        String serviceName = "service-name";
        DataServiceWriter writer = new DataServiceWriter(serviceName, dataUrl, ctx, locatorUrl);
        writer.connect();
        writer.process();

        ZMQ.Socket pull = ctx.createSocket(SocketType.PULL);
        pull.connect(dataUrl);

        writer.put("Payload".getBytes(StandardCharsets.UTF_8));

        String s = pull.recvStr();
        assertEquals("Payload", s);

        ZMsg msg = ZMsg.recvMsg(router);
        assertEquals(4, msg.size());
        msg.poll(); // ignore the identity message
        assertEquals("register", msg.poll().getString(ZMQ.CHARSET));
        assertEquals(serviceName, msg.poll().getString(ZMQ.CHARSET));
        assertEquals(dataUrl, msg.poll().getString(ZMQ.CHARSET));
    }
}