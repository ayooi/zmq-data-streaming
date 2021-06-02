package au.ooi.streams;

import org.junit.Test;
import org.zeromq.*;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataServiceLocatorTest {

    private String serviceUrl = "inproc://service-location-url";
    private String serviceName = "data-service";
    private String dataUrl = "inproc://data-service-url";
    private ZContext ctx = new ZContext();
    private DataServiceLocator dataServiceLocator;

    @Test
    public void testRegister() {
        dataServiceLocator = new DataServiceLocator(ctx, serviceUrl, 30);

        ZMQ.Socket socket = ctx.createSocket(SocketType.DEALER);
        socket.connect(serviceUrl);

        socket.sendMore("register");
        socket.sendMore(serviceName);
        socket.send(dataUrl);

        dataServiceLocator.process();
        List<ServiceLocation> query = dataServiceLocator.query(serviceName);
        assertEquals(1, query.size());
        assertEquals(dataUrl, query.get(0).getLocation());
    }

    @Test
    public void testQuery() {
        testRegister();

        ZMQ.Socket dealer = ctx.createSocket(SocketType.DEALER);
        dealer.connect(this.serviceUrl);
        dealer.sendMore("query");
        dealer.send(serviceName);
        dataServiceLocator.process();

        ZMsg zFrames = ZMsg.recvMsg(dealer);

        ZFrame poll = zFrames.poll();
        assertNotNull(poll);
        assertEquals(dataUrl, poll.getString(ZMQ.CHARSET));
    }
}