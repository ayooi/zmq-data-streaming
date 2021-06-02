package au.ooi.example;

import au.ooi.streams.DataServiceLocator;
import au.ooi.streams.DataServiceReader;
import au.ooi.streams.DataServiceWriter;
import au.ooi.streams.RealTimeProvider;
import org.zeromq.ZContext;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultipleWriters {
    public static final String SERVICE_LOCATOR_URL = "inproc://service-locator-url";
    public static final String SERVICE_NAME = "service-name";
    private static int count1 = 0;

    public static void main(String[] args) throws InterruptedException {
        ZContext ctx = new ZContext();
        DataServiceLocator dataServiceLocator = new DataServiceLocator(ctx, SERVICE_LOCATOR_URL, 10, new RealTimeProvider());
        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(dataServiceLocator);

        DataServiceWriter writer1 = new DataServiceWriter(SERVICE_NAME, "inproc://data-url-1", ctx, SERVICE_LOCATOR_URL);
        writer1.startup();
        executorService.submit(writer1);

        DataServiceReader reader1 = new DataServiceReader(SimpleReaderWriter.SERVICE_NAME, ctx, SERVICE_LOCATOR_URL);
        executorService.submit(reader1);

        executorService.submit(() -> {
            for (int i = 0; i < 5000000; i++) {
                writer1.put("Payload".getBytes(StandardCharsets.UTF_8));
            }
        });


        executorService.submit(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignored
            }
            System.out.println("Starting up writer 2...");
            DataServiceWriter writer2 = new DataServiceWriter(SERVICE_NAME, "inproc://data-url-2", ctx, SERVICE_LOCATOR_URL);
            writer2.startup();
            executorService.submit(writer2);
            for (int i = 0; i < 5000000; i++) {
                writer2.put("Payload".getBytes(StandardCharsets.UTF_8));
            }
        });

        do {
            reader1.take();
            count1++;
        } while (count1 < 10000000);

        System.out.printf("Successfully received 10000000 items %n");
        executorService.shutdownNow();
        ctx.close();
    }
}
