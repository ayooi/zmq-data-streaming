package au.ooi.streams;


import au.ooi.data.WriteConnectionDetail;
import org.zeromq.ZContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class DataServiceFactory implements DataServiceFactoryInterface {
    private final String serviceLocatorUrl;
    private final ZContext ctx;
    private final Map<String, DataServiceWriter> writers = new ConcurrentHashMap<>();
    private final ExecutorService executorService;

    public DataServiceFactory(ZContext ctx, String serviceLocatorUrl, ExecutorService executorService) {
        this.ctx = ctx;
        this.serviceLocatorUrl = serviceLocatorUrl;
        this.executorService = executorService;
    }

    @Override
    public DataServiceWriter createWriter(String url, String serviceName) {
        DataServiceWriter writer = new DataServiceWriter(serviceName, WriteConnectionDetail.parse(url), this.ctx, this.serviceLocatorUrl);
        DataServiceWriter existing = this.writers.putIfAbsent(serviceName, writer);
        if (existing == null) {
            writer.startup();
            this.executorService.submit(writer);
            return writer;
        } else {
            return existing;
        }
    }

    @Override
    public DataServiceReader createReader(String serviceName) {
        return new DataServiceReader(serviceName, this.ctx, this.serviceLocatorUrl);
    }

}
