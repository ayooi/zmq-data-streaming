package au.ooi.streams;

import au.ooi.data.ExpiredServiceDetails;
import au.ooi.data.ServiceLocations;
import au.ooi.externals.MutableTimeProvider;
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
    public void multipleRegister() {
        ServiceStore serviceStore = new ServiceStore(1, new MutableTimeProvider(Instant.EPOCH));
        boolean register = serviceStore.register("service-name", "location-1");
        assertTrue(register);
        register = serviceStore.register("service-name", "location-2");
        assertTrue(register);
        ServiceLocations query = serviceStore.query("service-name");
        assertTrue(query.getLocations().contains("location-1"));
        assertTrue(query.getLocations().contains("location-2"));
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

    @Test
    public void bounceTimeout() throws InterruptedException {
        MutableTimeProvider timeProvider = new MutableTimeProvider(Instant.EPOCH);
        ServiceStore serviceStore = new ServiceStore(5, timeProvider);
        serviceStore.register("service-name", "location");

        timeProvider.addSeconds(2); // t = 2
        serviceStore.processTimeout();

        assertFalse(serviceStore.hasEvents());

        assertFalse(serviceStore.register("service-name", "location"));

        timeProvider.addSeconds(4); // t = 6

        serviceStore.processTimeout();
        assertFalse(serviceStore.hasEvents());
    }

}