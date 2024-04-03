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

package software.amazon.jdbc.plugin.bluegreen;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostRole;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.SlidingExpirationCacheWithCleanupThread;
import software.amazon.jdbc.util.StringUtils;

public class BlueGreenStatusMonitor {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusMonitor.class.getName());
  private static final long DEFAULT_CHECK_INTERVAL_MS = TimeUnit.MINUTES.toMillis(5);
  private static final long CACHE_CLEANUP_NANO = TimeUnit.MINUTES.toNanos(1);
  private static final long CACHE_HOST_REPLACEMENT_EXPIRATION_NANO = TimeUnit.MINUTES.toNanos(2);

  private static final HashMap<String, BlueGreenPhases> blueGreenStatusMapping =
      new HashMap<String, BlueGreenPhases>() {
        {
          put("SOURCE", BlueGreenPhases.CREATED);
          put("TARGET", BlueGreenPhases.CREATED);
          put("SWITCHOVER_STARTING", BlueGreenPhases.SWITCHING_OVER);
          put("SWITCHOVER_IN_PROGRESS", BlueGreenPhases.SWITCHING_OVER);
          put("TARGET_PROMOTED_DNS_UPDATING", BlueGreenPhases.SWITCH_OVER_COMPLETED);
        }
      };

  protected static final SlidingExpirationCacheWithCleanupThread<String, HostReplacementHolder> hostReplacements =
      new SlidingExpirationCacheWithCleanupThread<>(
          BlueGreenStatusMonitor::canRemoveHostReplacement,
          null,
          CACHE_CLEANUP_NANO);

  protected final String statusQuery;
  protected final PluginService pluginService;
  protected final Properties props;

  protected final Map<BlueGreenPhases, Long> checkIntervalMap;
  protected final ExecutorService executorService = Executors.newFixedThreadPool(1);

  protected final RdsUtils rdsUtils = new RdsUtils();
  protected boolean canHoldConnection = false;
  protected Connection connection = null;
  protected Random random = new Random();

  public BlueGreenStatusMonitor(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull String statusQuery,
      final @NonNull Map<BlueGreenPhases, Long> checkIntervalMap) {

    this.pluginService = pluginService;
    this.props = props;
    this.statusQuery = statusQuery;
    this.checkIntervalMap = checkIntervalMap;

    executorService.submit(() -> {

      TimeUnit.SECONDS.sleep(5); // Some delay so the connection is initialized and topology is fetched.

      while (true) {
        try {
          final BlueGreenStatus status = this.getStatus();
          this.pluginService.setStatus(BlueGreenStatus.class, status, true);

          long delay = checkIntervalMap.getOrDefault(status.getCurrentPhase(), DEFAULT_CHECK_INTERVAL_MS);
          this.canHoldConnection = (delay <= TimeUnit.MINUTES.toMillis(2));
          TimeUnit.MILLISECONDS.sleep(delay);

        } catch (InterruptedException interruptedException) {
          throw interruptedException;
        } catch (Exception ex) {
          LOGGER.log(Level.FINEST, "Unhandled exception while monitoring blue/green status.", ex);
        }
      }
    });
    executorService.shutdown(); // executor accepts no more tasks
  }

  protected static boolean canRemoveHostReplacement(final HostReplacementHolder hostReplacementHolder) {
    try {
      final String currentIp = InetAddress.getByName(hostReplacementHolder.host).getHostAddress();
      if (!hostReplacementHolder.currentIp.equals(currentIp)) {
        // DNS has changed. We can remove this replacement.
        return true;
      }
    } catch (UnknownHostException ex) {
      // do nothing
    }
    return false;
  }

  // Collect IP addresses of green nodes and map them to a new-blue FQDN host names.
  // It's assumed that each blue nodes will be mapped to a single green node.
  // Example:
  // Current topology:
  // - instance-1.XYZ.us-east-2.rds.amazonaws.com (10.0.1.100)
  // - instance-2.XYZ.us-east-2.rds.amazonaws.com (10.0.1.101)
  // - instance-3.XYZ.us-east-2.rds.amazonaws.com (10.0.1.102)
  // - instance-1-green-123456.XYZ.us-east-2.rds.amazonaws.com (10.0.1.103)
  // - instance-2-green-234567.XYZ.us-east-2.rds.amazonaws.com (10.0.1.104)
  //  -instance-3-green-345678.XYZ.us-east-2.rds.amazonaws.com (10.0.1.105)
  //
  // Expected mapping:
  // - instance-1.XYZ.us-east-2.rds.amazonaws.com (10.0.1.103)
  // - instance-2.XYZ.us-east-2.rds.amazonaws.com (10.0.1.104)
  // - instance-3.XYZ.us-east-2.rds.amazonaws.com (10.0.1.105)
  protected void collectHostIpAddresses() throws SQLException {
    final List<HostSpec> hosts = this.pluginService.getHosts();

    for (HostSpec hostSpec : hosts) {
      if (this.rdsUtils.isGreenInstance(hostSpec.getHost())) {
        final String greenHost = hostSpec.getHost();
        final String newBlueHost = this.rdsUtils.removeGreenInstancePrefix(hostSpec.getHost());
        try {
          final String greenIp = InetAddress.getByName(greenHost).getHostAddress();
          final String newBlueIp = InetAddress.getByName(newBlueHost).getHostAddress();
          hostReplacements.remove(newBlueHost);
          hostReplacements.computeIfAbsent(
              newBlueHost,
              (key) -> new HostReplacementHolder(newBlueHost, newBlueIp, greenIp),
              CACHE_HOST_REPLACEMENT_EXPIRATION_NANO);
        } catch (UnknownHostException ex) {
          // do nothing
        }
      }
    }
  }

  protected Map<String, String> getHostIpAddresses() {
    return new ConcurrentHashMap<>(
        hostReplacements.getEntries().values().stream()
            .collect(Collectors.toMap(k -> k.host, v -> v.replacementIp)));
  }

  protected BlueGreenStatus getStatus() {
    try {
      if (this.connection == null || !this.connection.isValid(5)) {
        if (this.connection != null) {
          try {
            this.connection.close();
          } catch (SQLException sqlException) {
            // ignore
          }
        }
        final HostSpec hostSpec = this.getHostSpec();
        if (hostSpec == null) {
          LOGGER.finest("No node found for blue/green status check.");
          return new BlueGreenStatus(BlueGreenPhases.NOT_CREATED, new ConcurrentHashMap<>());
        }
        this.connection = this.getConnection(hostSpec);
      }

      final Statement statement = this.connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(this.statusQuery);

      final HashMap<String, BlueGreenPhases> phasesByHost = new HashMap<>();
      final HashSet<BlueGreenPhases> hostPhases = new HashSet<>();

      while (resultSet.next()) {
        final String endpoint = resultSet.getString("endpoint");
        final BlueGreenPhases phase = this.parsePhase(resultSet.getString("blue_green_deployment"));
        phasesByHost.put(endpoint, phase);
        hostPhases.add(phase);
      }

      BlueGreenPhases currentPhase;

//       // TODO: debug
//       final BlueGreenPhases finalCurrentPhase = BlueGreenStatusSimulator.getPhase();
//       phasesByHost.forEach((key, value) -> phasesByHost.put(key, finalCurrentPhase));
//       hostPhases.clear();
//       hostPhases.add(finalCurrentPhase);
//       // TODO: end debug


      if (hostPhases.contains(BlueGreenPhases.SWITCHING_OVER)) {
        // At least one node is in active switching over phase.
        currentPhase = BlueGreenPhases.SWITCHING_OVER;
      } else if (hostPhases.contains(BlueGreenPhases.SWITCH_OVER_COMPLETED)) {
        // All nodes have completed switchover.
        currentPhase = BlueGreenPhases.SWITCH_OVER_COMPLETED;
      } else if (hostPhases.contains(BlueGreenPhases.CREATED) && hostPhases.size() == 1) {
        // At least one node reports that Bleu/Green deployment is created.
        currentPhase = BlueGreenPhases.CREATED;
      } else {
        currentPhase = BlueGreenPhases.NOT_CREATED;
      }

      if (currentPhase == BlueGreenPhases.CREATED) {
        this.pluginService.refreshHostList(this.connection);
        this.collectHostIpAddresses();
      }

      LOGGER.finest("currentPhase: " + currentPhase);
      return new BlueGreenStatus(currentPhase, this.getHostIpAddresses());

    } catch (SQLException ex) {
      // do nothing
      LOGGER.log(Level.FINEST, "Unhandled exception.", ex);
    } catch (Exception e) {
      // do nothing
      LOGGER.log(Level.FINEST, "Unhandled exception.", e);
    } finally {
      if (!this.canHoldConnection && this.connection != null) {
        try {
          this.connection.close();
        } catch (SQLException sqlException) {
          // ignore
        }
        this.connection = null;
      }
    }

    LOGGER.finest("currentPhase: " + BlueGreenPhases.NOT_CREATED);
    return new BlueGreenStatus(BlueGreenPhases.NOT_CREATED, new ConcurrentHashMap<>());
  }

  protected BlueGreenPhases parsePhase(final String value) {
    if (StringUtils.isNullOrEmpty(value)) {
      return BlueGreenPhases.NOT_CREATED;
    }
    final BlueGreenPhases phase = blueGreenStatusMapping.get(value.toUpperCase());

    if (phase == null) {
      throw new IllegalArgumentException("Unknown blue/green status " + value);
    }
    return phase;
  }

  protected Connection getConnection(final HostSpec hostSpec) throws SQLException {
    return this.pluginService.forceConnect(hostSpec, this.props);
  }

  protected HostSpec getHostSpec() {

    final List<HostSpec> hosts = this.pluginService.getHosts();

    HostSpec hostSpec = this.getRandomHost(hosts, HostRole.UNKNOWN, true);

    if (hostSpec != null) {
      return hostSpec;
    }

    // There's no green nodes in topology.
    // Try to get any node.
    hostSpec = this.getRandomHost(hosts, HostRole.READER, false);

    if (hostSpec != null) {
      return hostSpec;
    }

    // There's no green nodes in topology.
    // Try to get any node.
    hostSpec = this.getRandomHost(hosts, HostRole.WRITER, false);

    if (hostSpec != null) {
      return hostSpec;
    }

    return this.getRandomHost(hosts, HostRole.UNKNOWN, false);
  }

  protected HostSpec getRandomHost(List<HostSpec> hosts, HostRole role, boolean greenHostOnly) {
    final List<HostSpec> filteredHosts = hosts.stream()
        .filter(x -> (x.getRole() == role || role == HostRole.UNKNOWN)
            && (!greenHostOnly || this.rdsUtils.isGreenInstance(x.getHost())))
        .collect(Collectors.toList());

    return filteredHosts.isEmpty()
        ? null
        : filteredHosts.get(this.random.nextInt(filteredHosts.size()));
  }

  public static class HostReplacementHolder {
    public final String host;
    public final String currentIp;
    public final String replacementIp;

    public HostReplacementHolder(final String host, final String currentIp, final String replacementIp) {
      this.host = host;
      this.currentIp = currentIp;
      this.replacementIp = replacementIp;
    }
  }
}
