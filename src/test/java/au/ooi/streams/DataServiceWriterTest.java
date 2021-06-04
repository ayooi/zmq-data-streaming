package au.ooi.streams;

import au.ooi.data.WriteConnectionDetail;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.junit.Assert.assertEquals;

public class DataServiceWriterTest {

    @Test
    public void testRegister() {
        ZContext ctx = new ZContext();

        ZMQ.Socket router = ctx.createSocket(SocketType.ROUTER);
        String locatorUrl = "inproc://service-location";
        router.bind(locatorUrl);
        String dataUrl = "inproc://random-location";
        String serviceName = "service-name";
        DataServiceWriter writer = new DataServiceWriter(serviceName, WriteConnectionDetail.parse(dataUrl), ctx, locatorUrl);
        writer.startup();
        writer.process();

        ZMsg msg = ZMsg.recvMsg(router);
        assertEquals(4, msg.size());
        msg.poll(); // ignore the identity message
        assertEquals("register", Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
        assertEquals(serviceName, Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
        assertEquals(dataUrl, Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
    }

    @Test
    public void testWrite() {
        ZContext ctx = new ZContext();

        ZMQ.Socket router = ctx.createSocket(SocketType.ROUTER);
        String locatorUrl = "inproc://service-location";
        router.bind(locatorUrl);
        String dataUrl = "inproc://random-location";
        String serviceName = "service-name";
        DataServiceWriter writer = new DataServiceWriter(serviceName, WriteConnectionDetail.parse(dataUrl), ctx, locatorUrl);
        writer.startup();

        ZMQ.Socket pull = ctx.createSocket(SocketType.PULL);
        pull.connect(dataUrl);

        writer.put("Payload".getBytes(StandardCharsets.UTF_8));

        String s = pull.recvStr();
        assertEquals("Payload", s);
    }
}