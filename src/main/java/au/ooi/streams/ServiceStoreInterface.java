package au.ooi.streams;

import au.ooi.data.ExpiredServiceDetails;
import au.ooi.data.ServiceLocations;

public interface ServiceStoreInterface {
    boolean remove(String serviceName, String location);

    boolean register(String serviceName, String location);

    ServiceLocations query(String serviceName);

    boolean hasEvents();

    ExpiredServiceDetails take() throws InterruptedException;
}
