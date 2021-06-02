package au.ooi.streams;

import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class ServiceStoreTest {

    @Test
    public void basicQuerying() {
        ServiceStore serviceStore = new ServiceStore(1, new MutableTimeProvider(Instant.EPOCH));
        serviceStore.register("service-name", "location");
        ServiceLocations query = serviceStore.query("service-name");
        assertTrue(query.getLocations().contains("location"));
    }

    @Test
    public void basicTimeout() throws InterruptedException {
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        ServiceStore serviceStore = new ServiceStore(1, timeProvider);
        serviceStore.register("service-name", "location");

        timeProvider.addSeconds(2);
        serviceStore.processTimeout();

        ExpiredServiceDetails take = serviceStore.take();
        assertEquals("service-name", take.getServiceName());
        assertEquals("location", take.getLocation());

        assertFalse(serviceStore.hasEvents());

        serviceStore.register("service-name", "location");
        serviceStore.processTimeout();
        assertFalse(serviceStore.hasEvents());
    }

}