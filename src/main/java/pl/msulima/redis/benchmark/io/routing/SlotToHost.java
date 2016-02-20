package pl.msulima.redis.benchmark.io.routing;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisClusterCRC16;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class SlotToHost {

    private static final int SLOTS_COUNT = 16384;
    public static final int ARGUMENTS_INDEX = 8;
    public static final int HOST_AND_PORT_INDEX = 1;

    private final HostAndPort[] slotToHost = new HostAndPort[SLOTS_COUNT];

    public SlotToHost(String host, int port) {
        try (Jedis jedis = new Jedis(host, port)) {
            String clusterNodes = jedis.clusterNodes();

            for (String s : clusterNodes.split("\\n")) {
                String[] strings = s.split("\\s");
                String[] hap = strings[HOST_AND_PORT_INDEX].split(":");
                HostAndPort hostAndPort = new HostAndPort(hap[0], Integer.parseInt(hap[1]));

                for (int i = ARGUMENTS_INDEX; i < strings.length; i++) {
                    String[] idRange = strings[i].split("-");
                    if (idRange.length == 1) {
                        updateRoutingTable(hostAndPort, Integer.parseInt(idRange[0]));
                    } else {
                        updateRoutingTable(hostAndPort, Integer.parseInt(idRange[0]), Integer.parseInt(idRange[1]));
                    }
                }
            }
        } catch (JedisDataException jde) {
            updateRoutingTable(new HostAndPort(host, port), 0, SLOTS_COUNT - 1);
        }
    }

    public void updateRoutingTable(HostAndPort hostAndPort, int rangeStart, int rangeEnd) {
        for (int slot = rangeStart; slot <= rangeEnd; slot++) {
            updateRoutingTable(hostAndPort, slot);
        }
    }

    public void updateRoutingTable(HostAndPort hostAndPort, int slot) {
        slotToHost[slot] = hostAndPort;
    }

    public static int getSlot(byte[][] arguments) {
        if (arguments.length == 0) {
            return ThreadLocalRandom.current().nextInt(SLOTS_COUNT);
        } else {
            return JedisClusterCRC16.getCRC16(arguments[0]) & (SLOTS_COUNT - 1);
        }
    }

    public Set<HostAndPort> allHosts() {
        return new HashSet<>(Arrays.asList(slotToHost));
    }

    public HostAndPort getHost(int slot) {
        return slotToHost[slot];
    }
}
