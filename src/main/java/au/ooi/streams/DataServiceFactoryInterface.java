package au.ooi.streams;

public interface DataServiceFactoryInterface {
    DataServiceWriter createWriter(String url, String serviceName);

    DataServiceReader createReader(String serviceName);
}
