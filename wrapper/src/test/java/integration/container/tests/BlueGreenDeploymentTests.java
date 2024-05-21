/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.container.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import integration.DatabaseEngine;
import integration.DatabaseEngineDeployment;
import integration.TestEnvironmentFeatures;
import integration.TestEnvironmentInfo;
import integration.TestInstanceInfo;
import integration.container.ConnectionStringHelper;
import integration.container.TestDriver;
import integration.container.TestDriverProvider;
import integration.container.TestEnvironment;
import integration.container.condition.EnableOnDatabaseEngine;
import integration.container.condition.EnableOnDatabaseEngineDeployment;
import integration.container.condition.EnableOnTestFeature;
import integration.util.AuroraTestUtility;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.jdbc.util.RdsUtils;

@TestMethodOrder(MethodOrderer.MethodName.class)
@EnableOnTestFeature(TestEnvironmentFeatures.BLUE_GREEN_DEPLOYMENT)
@EnableOnDatabaseEngineDeployment(DatabaseEngineDeployment.RDS_MULTI_AZ_INSTANCE)
@EnableOnDatabaseEngine(DatabaseEngine.MYSQL)
@Order(16)
public class BlueGreenDeploymentTests {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenDeploymentTests.class.getName());
  protected static final AuroraTestUtility auroraUtil = AuroraTestUtility.getUtility();
  protected static final RdsUtils rdsUtil = new RdsUtils();

  private static final String MYSQL_BG_STATUS_QUERY =
      "SELECT id, SUBSTRING_INDEX(endpoint, '.', 1) as hostId, endpoint, port, blue_green_deployment FROM mysql.rds_topology";

  public static class TimeHolder {
    public long startTime;
    public long endTime;
    public String error;

    public TimeHolder(long startTime, long endTime) {
      this.startTime = startTime;
      this.endTime = endTime;
    }

    public TimeHolder(long startTime, long endTime, String error) {
      this.startTime = startTime;
      this.endTime = endTime;
      this.error = error;
    }
  }

  public static class BlueGreenResults {
    public final AtomicLong startTime = new AtomicLong();
    public final AtomicLong threadsSyncTime = new AtomicLong();
    public final AtomicLong bgTriggerTime = new AtomicLong();
    public final AtomicLong directBlueLostConnectionTime = new AtomicLong();
    public final AtomicLong directBlueIdleLostConnectionTime = new AtomicLong();
    public final AtomicLong wrapperBlueIdleLostConnectionTime = new AtomicLong();
    public final AtomicLong wrapperGreenLostConnectionTime = new AtomicLong();
    public final AtomicLong dnsBlueChangedTime = new AtomicLong();
    public String dnsBlueError = null;
    public final AtomicLong dnsGreenRemovedTime = new AtomicLong();
    public final ConcurrentHashMap<String, Long> blueStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, Long> greenStatusTime = new ConcurrentHashMap<>();
    public final ConcurrentLinkedQueue<TimeHolder> blueWrapperConnectTimes = new ConcurrentLinkedQueue<>();
    public final ConcurrentLinkedQueue<TimeHolder> blueWrapperExecuteTimes = new ConcurrentLinkedQueue<>();
  }

  private final BlueGreenResults results = new BlueGreenResults();

  @TestTemplate
  @ExtendWith(TestDriverProvider.class)
  public void test_Switchover(TestDriver testDriver) throws SQLException, InterruptedException {

    this.results.startTime.set(System.nanoTime());

    final AtomicBoolean stop = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(9);
    final CountDownLatch finishLatch = new CountDownLatch(9);

    final ArrayList<Thread> threads = new ArrayList<>();

    final TestEnvironmentInfo info = TestEnvironment.getCurrent().getInfo();
    final TestInstanceInfo testInstance = info.getDatabaseInfo().getInstances().get(0);
    final String dbName = info.getDatabaseInfo().getDefaultDbName();

    final List<String> topologyInstances = this.getBlueGreenEndpoints(info.getBlueGreenDeploymentId());

    for (String host : topologyInstances) {
      final String hostId = host.substring(0, host.indexOf('.') - 1);
      assertNotNull(hostId);

      if (rdsUtil.isNoPrefixInstance(host)) {
        threads.add(getDirectBlueConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getDirectBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueIdleConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueExecutingConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperBlueNewConnectionMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getBlueDnsMonitoringThread(
            hostId, host, startLatch, stop, finishLatch));
      }
      if (rdsUtil.isGreenInstance(host)) {
//         threads.add(getDirectGreenTopologyMonitoringThread(
//             hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getWrapperGreenConnectivityMonitoringThread(
            hostId, host, testInstance.getPort(), dbName, startLatch, stop, finishLatch, results));
        threads.add(getGreenDnsMonitoringThread(
            hostId, host, startLatch, stop, finishLatch));
      }
    }

    threads.add(getBlueGreenSwitchoverTriggerThread(
        info.getBlueGreenDeploymentId(), startLatch, stop, finishLatch, results));

    assertEquals(startLatch.getCount(), threads.size());
    assertEquals(finishLatch.getCount(), threads.size());

    threads.forEach(Thread::start);
    LOGGER.finest("All threads started.");

    TimeUnit.SECONDS.sleep(10);
    startLatch.countDown();

    finishLatch.await(10, TimeUnit.MINUTES);
    LOGGER.finest("Test is over.");

    threads.forEach(Thread::interrupt);

    assertTrue(results.bgTriggerTime.get() > 0);

    LOGGER.finest(String.format("startTime: %d ms",
        TimeUnit.NANOSECONDS.toMillis(results.startTime.get() - results.bgTriggerTime.get())));
    LOGGER.finest(String.format("threadsSyncTime: %d ms",
        TimeUnit.NANOSECONDS.toMillis(results.threadsSyncTime.get() - results.bgTriggerTime.get())));
    LOGGER.finest("bgTriggerTime: 0 (T0) ms");

    if (results.directBlueIdleLostConnectionTime.get() == 0) {
      LOGGER.finest("directBlueIdleLostConnectionTime: -");
    } else {
      LOGGER.finest(String.format("directBlueIdleLostConnectionTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.directBlueIdleLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.directBlueLostConnectionTime.get() == 0) {
      LOGGER.finest("directBlueLostConnectionTime (SELECT 1): -");
    } else {
      LOGGER.finest(String.format("directBlueLostConnectionTime (SELECT 1): %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.directBlueLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.wrapperBlueIdleLostConnectionTime.get() == 0) {
      LOGGER.finest("wrapperBlueIdleLostConnectionTime: -");
    } else {
      LOGGER.finest(String.format("wrapperBlueIdleLostConnectionTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.wrapperBlueIdleLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.wrapperGreenLostConnectionTime.get() == 0) {
      LOGGER.finest("wrapperGreenLostConnectionTime (SELECT 1): -");
    } else {
      LOGGER.finest(String.format("wrapperGreenLostConnectionTime (SELECT 1): %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.wrapperGreenLostConnectionTime.get() - results.bgTriggerTime.get())));
    }

    if (results.dnsBlueChangedTime.get() == 0) {
      LOGGER.finest("dnsBlueChangedTime: -");
    } else {
      LOGGER.finest(String.format("dnsBlueChangedTime: %d ms %s",
          TimeUnit.NANOSECONDS.toMillis(results.dnsBlueChangedTime.get() - results.bgTriggerTime.get()),
          (results.dnsBlueError == null ? "" : ", error: " + results.dnsBlueError)));
    }

    results.blueWrapperConnectTimes.stream()
        .sorted(Comparator.comparingLong(x -> x.startTime))
        .skip(results.blueWrapperConnectTimes.size() - 3)
        .forEach(x -> LOGGER.finest(String.format("blueWrapperConnectTimes [starting at %d ms]: duration %d ms %s",
            TimeUnit.NANOSECONDS.toMillis(x.startTime - results.bgTriggerTime.get()),
            TimeUnit.NANOSECONDS.toMillis(x.endTime - x.startTime),
                (x.error == null ? "" : ", error: " + x.error))));

    results.blueWrapperExecuteTimes.stream()
        .sorted(Comparator.comparingLong(x -> x.startTime))
        .skip(results.blueWrapperExecuteTimes.size() - 3)
        .forEach(x -> LOGGER.finest(String.format("blueWrapperExecuteTimes [starting at %d ms]: duration %d ms %s",
            TimeUnit.NANOSECONDS.toMillis(x.startTime - results.bgTriggerTime.get()),
            TimeUnit.NANOSECONDS.toMillis(x.endTime - x.startTime),
            (x.error == null ? "" : ", error: " + x.error))));

    if (results.dnsGreenRemovedTime.get() == 0) {
      LOGGER.finest("dnsGreenRemovedTime: -");
    } else {
      LOGGER.finest(String.format("dnsGreenRemovedTime: %d ms",
          TimeUnit.NANOSECONDS.toMillis(results.dnsGreenRemovedTime.get() - results.bgTriggerTime.get())));
    }

    results.blueStatusTime.entrySet().stream()
        .sorted(Comparator.comparingLong(Entry::getValue))
        .forEach(x -> LOGGER.finest(String.format("[blue] %s: %d ms",
            x.getKey(), TimeUnit.NANOSECONDS.toMillis(x.getValue() - results.bgTriggerTime.get()))));

    results.greenStatusTime.entrySet().stream()
        .sorted(Comparator.comparingLong(Entry::getValue))
        .forEach(x -> LOGGER.finest(String.format("[green] %s: %d ms",
            x.getKey(), TimeUnit.NANOSECONDS.toMillis(x.getValue() - results.bgTriggerTime.get()))));
  }

  // Blue node
  // Checking: connectivity, isClosed()
  private Thread getDirectBlueIdleConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest("[DirectBlueIdleConnectivity] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectBlueIdleConnectivity] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.directBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[DirectBlueIdleConnectivity] thread exception: " + throwable);
            results.directBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectBlueIdleConnectivity] thread unhandled exception: ", exception);
        fail("[DirectBlueConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[DirectBlueIdleConnectivity] thread is completed.");
      }
    });
  }

  // Blue node
  // Checking: connectivity, SELECT 1
  private Thread getDirectBlueConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {
    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(host, port, dbName),
            props);
        LOGGER.finest("[DirectBlueConnectivity] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectBlueConnectivity] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            final Statement statement = conn.createStatement();
            final ResultSet result = statement.executeQuery("SELECT 1");
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[DirectBlueConnectivity] thread exception: " + throwable);
            results.directBlueLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectBlueConnectivity] thread unhandled exception: ", exception);
        fail("[DirectBlueConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[DirectBlueConnectivity] thread is completed.");
      }
    });
  }

  // Blue node
  // Check: connectivity, isClosed()
  private Thread getWrapperBlueIdleConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrlWithPlugins(host, port, dbName, "bg"),
            props);
        LOGGER.finest("[WrapperBlueIdle] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueIdle] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            if (conn.isClosed()) {
              results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
              break;
            }
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[WrapperBlueIdle] thread exception: " + throwable);
            results.wrapperBlueIdleLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueIdle] thread unhandled exception: ", exception);
        fail("[WrapperBlueIdle] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueIdle] thread is completed.");
      }
    });
  }

  // Blue node
  // Check: connectivity, SELECT sleep(5)
  // Expect: long execution time (longer than 5s) during active phase of switchover
  private Thread getWrapperBlueExecutingConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = DriverManager.getConnection(
            ConnectionStringHelper.getUrlWithPlugins(host, port, dbName, "bg"),
            props);
        Statement statement = conn.createStatement();
        LOGGER.finest("[WrapperBlueExecute] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueExecute] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            ResultSet rs = statement.executeQuery("SELECT sleep(5)");
            endTime = System.nanoTime();
            results.blueWrapperExecuteTimes.add(new TimeHolder(startTime, endTime));
          } catch (SQLException throwable) {
            //LOGGER.finest("[WrapperBlueExecute] thread exception: " + throwable);
            endTime = System.nanoTime();
            results.blueWrapperExecuteTimes.add(new TimeHolder(startTime, endTime, throwable.getMessage()));
            if (conn.isClosed()) {
              break;
            }
          }

          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueExecute] thread unhandled exception: ", exception);
        fail("[WrapperBlueExecute] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueExecute] thread is completed.");
      }
    });
  }

  // Blue node
  // Check: connectivity, opening a new connection
  // Expect: long opening connection time during active phase of switchover
  private Thread getWrapperBlueNewConnectionMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperBlueNewConnection] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          long startTime = System.nanoTime();
          long endTime;
          try  {
            conn = DriverManager.getConnection(
                ConnectionStringHelper.getUrlWithPlugins(host, port, dbName, "bg"),
                props);
            endTime = System.nanoTime();
            results.blueWrapperConnectTimes.add(new TimeHolder(startTime, endTime));

          } catch (SQLException throwable) {
            //LOGGER.finest("[WrapperBlueNewConnection] thread exception: " + throwable);
            endTime = System.nanoTime();
            results.blueWrapperConnectTimes.add(new TimeHolder(startTime, endTime, throwable.getMessage()));
          }

          try {
            conn.close();
            conn = null;
          } catch (SQLException sqlException) {
            // do nothing
          }

          TimeUnit.MILLISECONDS.sleep(1000);
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperBlueNewConnection] thread unhandled exception: ", exception);
        fail("[WrapperBlueNewConnection] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperBlueNewConnection] thread is completed.");
      }
    });
  }

  // Green node
  // Check: DNS record presence
  // Expect: DNS record becomes deleted while/after switchover
  private Thread getGreenDnsMonitoringThread(
      final String hostId,
      final String host,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        final String ip = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[GreenDNS] %s -> %s", host, ip));

        while (!stop.get()) {
          TimeUnit.SECONDS.sleep(1);
          try {
            String tmp = InetAddress.getByName(host).getHostAddress();
          } catch (UnknownHostException unknownHostException) {
            results.dnsGreenRemovedTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException e) {
        // do nothing
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, "[GreenDNS] thread unhandled exception: ", e);
        fail(String.format("[GreenDNS] %s: thread unhandled exception: %s", hostId, e));
      } finally {
        finishLatch.countDown();
        LOGGER.finest("[GreenDNS] thread is completed.");
      }
    });
  }

  // Blue DNS
  // Check IP address change time
  public Thread getBlueDnsMonitoringThread(
      final String hostId,
      final String host,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch) {

    return new Thread(() -> {
      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        final String originalIp = InetAddress.getByName(host).getHostAddress();
        LOGGER.finest(() -> String.format("[BlueDNS] %s -> %s", host, originalIp));

        while (!stop.get()) {
          TimeUnit.SECONDS.sleep(1);
          try {
            String currentIp = InetAddress.getByName(host).getHostAddress();
            if (!currentIp.equals(originalIp)) {
              results.dnsBlueChangedTime.set(System.nanoTime());
              LOGGER.finest(() -> String.format("[BlueDNS] %s -> %s", host, currentIp));
              break;
            }
          } catch (UnknownHostException unknownHostException) {
            LOGGER.finest(() -> String.format("Error: %s", unknownHostException));
            results.dnsBlueError = unknownHostException.getMessage();
            results.dnsBlueChangedTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException e) {
        // do nothing
      } catch (Exception e) {
        LOGGER.log(Level.FINEST, "[BlueDNS] thread unhandled exception: ", e);
        fail(String.format("[BlueDNS] %s: thread unhandled exception: %s", hostId, e));
      } finally {
        finishLatch.countDown();
        LOGGER.finest("[BlueDNS] thread is completed.");
      }
    });
  }

  // Green node
  // Monitor BG status changes
  private Thread getDirectGreenTopologyMonitoringThread(
      final String hostId,
      final String url,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      final HashMap<String /* endpoint */, String /* BG status */> statusByHost = new HashMap<>();
      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrl(url, port, dbName),
            props);
        LOGGER.finest(String.format("[DirectGreenStatuses] connection open: %s", hostId));

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[DirectGreenStatuses] Starting BG statuses monitoring through " + hostId);

        while (!stop.get()) {
          if (conn == null) {
            conn = openConnectionWithRetry(
                ConnectionStringHelper.getUrl(url, port, dbName),
                props);
          }
          try {
            final Statement statement = conn.createStatement();
            final ResultSet rs = statement.executeQuery(MYSQL_BG_STATUS_QUERY);
            while (rs.next()) {
              String queryHostId = rs.getString("hostId");
              String oldStatus = statusByHost.get(queryHostId);
              String queryNewStatus = rs.getString("blue_green_deployment");
              boolean isGreen = rdsUtil.isGreenInstance(queryHostId);

              if (hostId.equalsIgnoreCase(queryHostId)) {
                if (oldStatus == null) {
                  statusByHost.put(queryHostId, queryNewStatus);
                } else if (!oldStatus.equalsIgnoreCase(queryNewStatus)) {
                  if (isGreen) {
                    results.greenStatusTime.put(queryNewStatus, System.nanoTime());
                  } else {
                    results.blueStatusTime.put(queryNewStatus, System.nanoTime());
                  }
                  statusByHost.put(queryHostId, queryNewStatus);
                }
              }
            }
            TimeUnit.MILLISECONDS.sleep(100);

          } catch (SQLException throwable) {
            LOGGER.finest(String.format("[DirectGreenStatuses] %s: thread exception: %s", hostId, throwable));
            try {
              if (conn != null && !conn.isClosed()) {
                conn.close();
              }
            } catch (Exception ex) {
              // do nothing
            }
            conn = null;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[DirectGreenStatuses] thread unhandled exception: ", exception);
        fail(String.format("[DirectGreenStatuses] %s: thread unhandled exception: %s", hostId, exception));
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest(String.format("[DirectGreenStatuses] %s: thread is completed.", hostId));
      }
    });
  }

  // Green node
  // Check: connectivity, SELECT 1
  // Expect: no interruption
  private Thread getWrapperGreenConnectivityMonitoringThread(
      final String hostId,
      final String host,
      final int port,
      final String dbName,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      Connection conn = null;
      try {
        final Properties props = ConnectionStringHelper.getDefaultProperties();
        conn = openConnectionWithRetry(
            ConnectionStringHelper.getUrlWithPlugins(host, port, dbName, "bg"),
            props);
        LOGGER.finest("[WrapperGreenConnectivity] connection is open.");

        Thread.sleep(1000);

        // notify that this thread is ready for work
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);

        LOGGER.finest("[WrapperGreenConnectivity] Starting connectivity monitoring " + hostId);

        while (!stop.get()) {
          try  {
            final Statement statement = conn.createStatement();
            final ResultSet result = statement.executeQuery("SELECT 1");
            TimeUnit.SECONDS.sleep(1);
          } catch (SQLException throwable) {
            LOGGER.finest("[WrapperGreenConnectivity] thread exception: " + throwable);
            results.wrapperGreenLostConnectionTime.set(System.nanoTime());
            break;
          }
        }

      } catch (InterruptedException interruptedException) {
        // Ignore, stop the thread
      } catch (Exception exception) {
        LOGGER.log(Level.FINEST, "[WrapperGreenConnectivity] thread unhandled exception: ", exception);
        fail("[WrapperGreenConnectivity] thread unhandled exception: " + exception);
      } finally {
        try {
          if (conn != null && !conn.isClosed()) {
            conn.close();
          }
        } catch (Exception ex) {
          // do nothing
        }

        finishLatch.countDown();
        LOGGER.finest("[WrapperGreenConnectivity] thread is completed.");
      }
    });
  }

  // RDS API, trigger BG switchover
  private Thread getBlueGreenSwitchoverTriggerThread(
      final String blueGreenId,
      final CountDownLatch startLatch,
      final AtomicBoolean stop,
      final CountDownLatch finishLatch,
      final BlueGreenResults results) {

    return new Thread(() -> {

      try {
        startLatch.countDown();

        // wait for another threads to be ready to start the test
        startLatch.await(5, TimeUnit.MINUTES);
        results.threadsSyncTime.set(System.nanoTime());

        TimeUnit.SECONDS.sleep(30);
        auroraUtil.switchoverBlueGreenDeployment(blueGreenId);
        results.bgTriggerTime.set(System.nanoTime());

        // let other threads to monitor enough time during switchover and after
        // assuming that switchover can be completed within 5min
        TimeUnit.MINUTES.sleep(5);
        stop.set(true);

      } catch (InterruptedException e) {
        // do nothing
      } finally {
        finishLatch.countDown();
        LOGGER.finest("[Switchover] thread is completed.");
      }
    });
  }

  private Connection openConnectionWithRetry(String url, Properties props) {
    Connection conn = null;
    int connectCount = 0;
    while (conn == null && connectCount < 10) {
      try {
        conn = DriverManager.getConnection(url, props);

      } catch (SQLException sqlEx) {
        // ignore, try to connect again
      }
      connectCount++;
    }

    if (conn == null) {
      fail("Can't connect to " + url);
    }
    return conn;
  }

  private List<String> getBlueGreenEndpoints(final String blueGreenId) {

    BlueGreenDeployment blueGreenDeployment = auroraUtil.getBlueGreenDeployment(blueGreenId);
    if (blueGreenDeployment == null) {
      throw new RuntimeException("BG not found: " + blueGreenId);
    }
    DBInstance blueInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.source());
    if (blueInstance == null) {
      throw new RuntimeException("Blue instance not found.");
    }
    DBInstance greenInstance = auroraUtil.getRdsInstanceInfoByArn(blueGreenDeployment.target());
    if (greenInstance == null) {
      throw new RuntimeException("Blue instance not found.");
    }
    return Arrays.asList(blueInstance.endpoint().address(), greenInstance.endpoint().address());
  }
}
