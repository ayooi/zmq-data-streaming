package au.ooi.data;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WriteConnectionDetailTest {

    @Test
    public void parseTcp() {
        WriteConnectionDetail parse = WriteConnectionDetail.parse("tcp://some.name:2395");
        assertEquals(WriteConnectionType.TCP, parse.getType());
        assertEquals("some.name", parse.getAddress());
        assertEquals(2395, parse.getListenPort());
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseLackingColons() {
        WriteConnectionDetail.parse("tcp//some.name:3359");
    }

    @Test(expected = IllegalArgumentException.class)
    public void parseLackingListenPort() {
        WriteConnectionDetail.parse("tcp://some.name3359");
    }

    @Test
    public void parseIpc() {
        WriteConnectionDetail parse = WriteConnectionDetail.parse("ipc://some.name:2395");
        assertEquals(WriteConnectionType.IPC, parse.getType());
        assertEquals("some.name:2395", parse.getAddress());
        assertEquals(0, parse.getListenPort());
    }

    @Test
    public void parseInproc() {
        WriteConnectionDetail parse = WriteConnectionDetail.parse("inproc://some.name:2395");
        assertEquals(WriteConnectionType.INPROC, parse.getType());
        assertEquals("some.name:2395", parse.getAddress());
        assertEquals(0, parse.getListenPort());
    }

}