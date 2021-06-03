package au.ooi.streams;

import au.ooi.externals.MutableTimeProvider;
import au.ooi.externals.RealTimeProvider;
import org.junit.Test;
import org.zeromq.*;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.Assert.*;

public class DataServiceLocatorTest {

    private String serviceUrl = "inproc://service-location-url";
    private String serviceName = "data-service";
    private String dataUrl = "inproc://data-service-url";
    private ZContext ctx = new ZContext();
    private DataServiceLocator dataServiceLocator;

    @Test
    public void testSingleRegister() {
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        dataServiceLocator = new DataServiceLocator(ctx, serviceUrl, timeProvider, new ServiceStore(30, timeProvider));

        ZMQ.Socket socket = ctx.createSocket(SocketType.DEALER);
        socket.connect(serviceUrl);

        socket.sendMore("register");
        socket.sendMore(serviceName);
        socket.send(dataUrl);

        dataServiceLocator.process();
        List<String> query = dataServiceLocator.query(serviceName);
        assertEquals(1, query.size());
        assertEquals(dataUrl, query.get(0));
    }

    @Test
    public void testBasicRemoval() {
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        ServiceStore serviceStore = new ServiceStore(30, timeProvider);
        dataServiceLocator = new DataServiceLocator(ctx, serviceUrl, timeProvider, serviceStore);

        ZMQ.Socket socket = ctx.createSocket(SocketType.DEALER);
        socket.connect(serviceUrl);

        new ZMsg().addLast("register")
                .addLast(serviceName)
                .addLast(dataUrl)
                .send(socket);

        dataServiceLocator.process();
        assertFalse(serviceStore.query(serviceName).getLocations().isEmpty());

        new ZMsg().addLast("deregister")
                .addLast(serviceName)
                .addLast(dataUrl)
                .send(socket);

        dataServiceLocator.process();
        assertTrue(serviceStore.query(serviceName).getLocations().isEmpty());
    }

    @Test
    public void testMultipleRegister() {
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        dataServiceLocator = new DataServiceLocator(ctx, serviceUrl, timeProvider, new ServiceStore(30, timeProvider));

        ZMQ.Socket socket = ctx.createSocket(SocketType.DEALER);
        socket.setIdentity("identity-1".getBytes(StandardCharsets.UTF_8));
        socket.connect(serviceUrl);

        socket.sendMore("register");
        socket.sendMore(serviceName);
        socket.send("inproc://data-url-1");

        socket.sendMore("register");
        socket.sendMore(serviceName);
        socket.send("inproc://data-url-2");

        dataServiceLocator.process();
        dataServiceLocator.process();

        List<String> query = dataServiceLocator.query(serviceName);
        assertEquals(2, query.size());
        assertEquals("inproc://data-url-1", query.get(0));
        assertEquals("inproc://data-url-2", query.get(1));

        socket.sendMore("query");
        socket.send(serviceName);
        dataServiceLocator.process();
        ZMsg msg = ZMsg.recvMsg(socket);
        assertEquals(2, msg.size());

        socket.sendMore("query");
        socket.send(serviceName);
        dataServiceLocator.process();
        ZMQ.Poller poller = ctx.createPoller(1);
        poller.register(socket, ZMQ.Poller.POLLIN);

        poller.poll(0);
        assertFalse(poller.pollin(0));

        // Register a 3rd
        socket.sendMore("register");
        socket.sendMore(serviceName);
        socket.send("inproc://data-url-3");
        dataServiceLocator.process();

        poller.poll(0);
        assertTrue(poller.pollin(0));
    }

    @Test
    public void testQuery() {
        testSingleRegister();

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