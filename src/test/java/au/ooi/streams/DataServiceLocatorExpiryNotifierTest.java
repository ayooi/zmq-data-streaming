package au.ooi.streams;

import au.ooi.data.ExpiredServiceDetails;
import au.ooi.data.ServiceLocations;
import org.junit.Test;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.Assert.*;

public class DataServiceLocatorExpiryNotifierTest {

    static class FakeServiceStore implements ServiceStoreInterface {

        private final ArrayBlockingQueue<ExpiredServiceDetails> foo = new ArrayBlockingQueue<>(100);

        public void add(ExpiredServiceDetails expiredServiceDetails) {
            this.foo.add(expiredServiceDetails);
        }

        @Override
        public boolean remove(String serviceName, String location) {
            return false;
        }

        @Override
        public boolean register(String serviceName, String location) {
            return false;
        }

        @Override
        public ServiceLocations query(String serviceName) {
            return null;
        }

        @Override
        public boolean hasEvents() {
            return false;
        }

        @Override
        public ExpiredServiceDetails take() throws InterruptedException {
            return this.foo.take();
        }
    }

    @Test
    public void foo() throws InterruptedException {
        ZContext ctx = new ZContext();
        ZMQ.Socket socket = ctx.createSocket(SocketType.ROUTER);
        String url = "inproc://service-locator";
        socket.bind(url);
        FakeServiceStore store = new FakeServiceStore();
        DataServiceLocatorExpiryNotifier notifier = new DataServiceLocatorExpiryNotifier(url, ctx, store);

        store.add(new ExpiredServiceDetails("name", "location"));
        notifier.process();

        ZMsg msg = ZMsg.recvMsg(socket);
        assertEquals(4, msg.size());
        msg.poll(); // don't care about the first identity message
        assertEquals("deregister", Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
        assertEquals("name", Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
        assertEquals("location", Objects.requireNonNull(msg.poll()).getString(ZMQ.CHARSET));
    }
}