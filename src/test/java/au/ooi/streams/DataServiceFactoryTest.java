package au.ooi.streams;

import au.ooi.externals.MutableTimeProvider;
import org.junit.Test;
import org.zeromq.ZContext;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataServiceFactoryTest {
    @Test
    public void foo() throws InterruptedException {
        String serviceLocatorUrl = "inproc://service-locator";
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        ZContext ctx = new ZContext();
        ExecutorService executorService = Executors.newCachedThreadPool();
        DataServiceLocator dataServiceLocator = new DataServiceLocator(ctx, serviceLocatorUrl, timeProvider, new ServiceStore(10, timeProvider));
        executorService.submit(dataServiceLocator);
        DataServiceFactory dataServiceFactory = new DataServiceFactory(ctx, serviceLocatorUrl, executorService);
        String serviceName = "service-name";
        DataServiceWriter writer = dataServiceFactory.createWriter("inproc://data-location", serviceName);
        DataServiceReader reader = dataServiceFactory.createReader(serviceName);
        executorService.submit(reader.getRunnable());
        writer.put("Payload".getBytes(StandardCharsets.UTF_8));

        reader.process();
        assertTrue(reader.hasData());
        assertEquals("Payload", new String(reader.take()));
    }
}