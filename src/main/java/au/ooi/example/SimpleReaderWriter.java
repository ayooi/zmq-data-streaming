package au.ooi.example;

import au.ooi.streams.DataServiceLocator;
import au.ooi.streams.DataServiceReader;
import au.ooi.streams.DataServiceWriter;
import au.ooi.streams.RealTimeProvider;
import org.zeromq.ZContext;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SimpleReaderWriter {

    public static final String SERVICE_LOCATOR_URL = "inproc://service-locator-url";
    public static final String SERVICE_NAME = "service-name";

    public static void main(String[] args) throws InterruptedException {
        ZContext ctx = new ZContext();
        DataServiceLocator dataServiceLocator = new DataServiceLocator(ctx, SERVICE_LOCATOR_URL, 10, new RealTimeProvider());
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(dataServiceLocator);

        DataServiceWriter writer = new DataServiceWriter(SERVICE_NAME, "inproc://data-url", ctx, SERVICE_LOCATOR_URL);
        writer.startup();
        executorService.submit(writer);

        DataServiceReader reader = new DataServiceReader(SimpleReaderWriter.SERVICE_NAME, ctx, SERVICE_LOCATOR_URL);
        executorService.submit(reader);

        new Thread(() -> {
            while(true) {
                writer.put("Payload".getBytes(StandardCharsets.UTF_8));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // ignored
                }
            }
        }).start();

        while (true) {
            System.out.println(new String(reader.take()));
        }

    }
}
