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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.JdbcCallable;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.hostavailability.HostAvailability;
import software.amazon.jdbc.plugin.AbstractConnectionPlugin;
import software.amazon.jdbc.util.RdsUtils;
import software.amazon.jdbc.util.StringUtils;
import software.amazon.jdbc.util.SubscribedMethodHelper;
import software.amazon.jdbc.util.WrapperUtils;
import software.amazon.jdbc.util.telemetry.TelemetryContext;
import software.amazon.jdbc.util.telemetry.TelemetryFactory;
import software.amazon.jdbc.util.telemetry.TelemetryTraceLevel;

public class BlueGreenConnectionPlugin extends AbstractConnectionPlugin {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenConnectionPlugin.class.getName());
  private static final String TELEMETRY_SWITCHOVER = "Blue/Green switchover";

  public static final AwsWrapperProperty BG_CONNECT_TIMEOUT = new AwsWrapperProperty(
      "bgConnectTimeout", "30000",
      "Connect timeout (in msec) during Blue/Green Deployment switchover.");

  protected static final ReentrantLock providerInitLock = new ReentrantLock();
  protected static BlueGreenStatusProvider provider = null;

  private static final Set<String> CLOSING_METHOD_NAMES = Collections.unmodifiableSet(
      new HashSet<>(Arrays.asList(
          "Connection.close",
          "Connection.abort",
          "Statement.close",
          "CallableStatement.close",
          "PreparedStatement.close",
          "ResultSet.close"
      )));

  static {
    PropertyDefinition.registerPluginProperties(BlueGreenConnectionPlugin.class);
  }

  protected final PluginService pluginService;
  protected final Properties props;
  protected final Set<String> subscribedMethods;
  protected Supplier<BlueGreenStatusProvider> initProviderSupplier;

  protected final TelemetryFactory telemetryFactory;
  protected final RdsUtils rdsUtils = new RdsUtils();


  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {
    this(pluginService, props, () -> new BlueGreenStatusProvider(pluginService, props));
  }

  public BlueGreenConnectionPlugin(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props,
      final @NonNull Supplier<BlueGreenStatusProvider> initProviderSupplier) {

    this.pluginService = pluginService;
    this.props = props;
    this.telemetryFactory = pluginService.getTelemetryFactory();
    this.initProviderSupplier = initProviderSupplier;

    final Set<String> methods = new HashSet<>();
    /*
     We should NOT to subscribe to "forceConnect" pipeline since it's used by
     BG monitoring and we don't want to block those monitoring connection.
    */
    methods.add("connect");
    methods.addAll(SubscribedMethodHelper.NETWORK_BOUND_METHODS);
    this.subscribedMethods = Collections.unmodifiableSet(methods);
  }

  @Override
  public Set<String> getSubscribedMethods() {
    return this.subscribedMethods;
  }

  @Override
  public Connection connect(
      final String driverProtocol,
      final HostSpec hostSpec,
      final Properties props,
      final boolean isInitialConnection,
      final JdbcCallable<Connection, SQLException> connectFunc)
      throws SQLException {

    final HostSpec routedHostSpec = this.rejectOrHoldConnect(hostSpec);

    if (routedHostSpec == null) {
      Connection conn;
      try {
        conn = connectFunc.call();
      } catch (final SQLException exception) {
        throw exception;
      }

      // Provider should be initialized after connection is open and a dialect is properly identified.
      this.initProvider();

      return conn;

    } else {
      return this.pluginService.connect(routedHostSpec, props);
    }
  }

  @Override
  public <T, E extends Exception> T execute(
      final Class<T> resultClass,
      final Class<E> exceptionClass,
      final Object methodInvokeOn,
      final String methodName,
      final JdbcCallable<T, E> jdbcMethodFunc,
      final Object[] jdbcMethodArgs)
      throws E {

    this.initProvider();

    if (!CLOSING_METHOD_NAMES.contains(methodName)) {
      try {
        this.rejectOrHoldExecute(methodName);
      } catch (SQLException ex) {
        throw WrapperUtils.wrapExceptionIfNeeded(exceptionClass, ex);
      }
    }

    return jdbcMethodFunc.call();
  }

  protected long getNanoTime() {
    return System.nanoTime();
  }

  protected HostSpec rejectOrHoldConnect(final HostSpec connectHostSpec)
      throws SQLTimeoutException {

    BlueGreenStatus bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

    if(bgStatus != null) {

      if (bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

        if (this.isBeforeSourceHostSpec(connectHostSpec) || this.isBeforeTargetHostSpec(connectHostSpec)) {
          // hold this call till BG switchover is completed.
          this.holdOn(bgStatus, "connect");
          return this.replaceToTargetIp(bgStatus, connectHostSpec);
        }
      } else if (bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCH_OVER_COMPLETED) {

        if (this.isAfterSourceHostSpec(connectHostSpec)) {
          return this.replaceToTargetIp(bgStatus, connectHostSpec);
        }
        if (this.isAfterDeprecatedHostSpec(connectHostSpec)) {
          return this.replaceToTargetIp(bgStatus, connectHostSpec);
        }
      }
    }
    return null;
  }

  // Replaces hostSpec based on FQDN host name with a new one that is uses IP address.
  // BlueGreenStatus contains a map of host names (FQDN) to corresponding IP addresses.
  // The mapping represents an assumption about what new-blue nodes and their IP addresses
  // look like right after switchover. The mapping doesn't contain green nodes since they are deprecated as
  // soon as switchover is over. The logic below assumes that source cluster and target cluster contain
  // the same number of nodes and blue node could be bound with a single target green node.
  // Such logic should be reviewed for further Blue/Green versions that allows more options
  // for source and target clusters.
  protected HostSpec replaceToTargetIp(final BlueGreenStatus bgStatus, final HostSpec hostSpec) {
    if (this.rdsUtils.isIPv4(hostSpec.getHost()) || this.rdsUtils.isIPv6(hostSpec.getHost())) {
      return null;
    }
    String host = this.rdsUtils.isGreenInstance(hostSpec.getHost())
        ? hostSpec.getHost()
        : this.rdsUtils.removeGreenInstancePrefix(hostSpec.getHost());
    String replacementId = bgStatus.getHostIpAddresses().get(host);

    if (replacementId == null) {
      // Get random IP
      final Random rnd = new Random();
      final String randomKey = bgStatus.getHostIpAddresses().keySet()
          .toArray(new String[0])[rnd.nextInt(bgStatus.getHostIpAddresses().size())];
      if (randomKey != null) {
        replacementId = bgStatus.getHostIpAddresses().get(randomKey);
        host = randomKey;
      } else {
        return null;
      }
    }

    final HostSpec resultHostSpec = this.pluginService.getHostSpecBuilder().copyFrom(hostSpec)
        .host(replacementId)
        .hostId(this.getHostId(host))
        .availability(HostAvailability.AVAILABLE)
        .build();

    LOGGER.finest("reroute to IP=" + replacementId);
    return resultHostSpec;
  }

  protected String getHostId(final String endpoint) {
    if (StringUtils.isNullOrEmpty(endpoint)) {
      return null;
    }
    final String[] parts = endpoint.split(".");
    return parts != null && parts.length > 0 ? parts[0] : null;
  }

  protected void rejectOrHoldExecute(final String methodName)
      throws SQLException {

    BlueGreenStatus bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);

    // TODO: debug
    LOGGER.finest("status=" + (bgStatus == null ? "null" : bgStatus.getCurrentPhase()));

    if (bgStatus != null) {

      if (bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

        if (this.isBeforeSourceHostSpec(this.pluginService.getCurrentHostSpec())) {
          if (this.pluginService.getCurrentConnection() != null) {
            this.pluginService.getCurrentConnection().close();
          }
          throw new SQLException(
              "Connection to blue node has been closed since Blue/Green switchover is in progress.", "08001");

        } else if (this.isBeforeTargetHostSpec(this.pluginService.getCurrentHostSpec())) {
          //TODO: there are two options for green node traffic:
          // 1) close green connection
/*
           if (this.pluginService.getCurrentConnection() != null) {
             this.pluginService.getCurrentConnection().close();
           }
           throw new SQLException(
               "Connection to green node has been closed since Blue/Green switchover is in progress.",
               "08001");
*/

          // 2) hold all traffic to green nodes
          this.holdOn(bgStatus, methodName);

        }
      } else if (bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCH_OVER_COMPLETED) {

        if (this.isAfterSourceHostSpec(this.pluginService.getCurrentHostSpec())) {
          // continue with the current call
        }
        else if (this.isAfterDeprecatedHostSpec(this.pluginService.getCurrentHostSpec())) {
          //TODO: there are two options for green node traffic:
          // 1) close green connection
/*
          if (this.pluginService.getCurrentConnection() != null) {
            this.pluginService.getCurrentConnection().close();
          }
          throw new SQLException(
              "Connection to green node has been closed since Blue/Green switchover is completed "
              + " and green endpoints are deprecated.", "08001");
*/
          // 2) continue with open connections
          // do nothing
        }
      }
    }
  }

  protected boolean isBeforeSourceHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isNoPrefixInstance(host);
  }

  protected boolean isBeforeTargetHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isGreenInstance(host);
  }

  protected boolean isAfterSourceHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isNoPrefixInstance(host);
  }

  protected boolean isAfterDeprecatedHostSpec(final HostSpec hostSpec) {
    final String host = hostSpec.getHost();
    return this.rdsUtils.isGreenInstance(host);
  }

  protected void holdOn(BlueGreenStatus bgStatus, final String methodName) throws SQLTimeoutException {

    LOGGER.finest(String.format(
        "Blue/Green Deployment switchover is in progress. Hold %s call until it's completed.", methodName));

    TelemetryContext telemetryContext = telemetryFactory.openTelemetryContext(TELEMETRY_SWITCHOVER,
        TelemetryTraceLevel.NESTED);

    try {
      long endTime = this.getNanoTime() + TimeUnit.MILLISECONDS.toNanos(BG_CONNECT_TIMEOUT.getLong(props));

      while (this.getNanoTime() <= endTime
          && bgStatus != null
          && bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {

        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        bgStatus = this.pluginService.getStatus(BlueGreenStatus.class, true);
      }

      if (bgStatus != null && bgStatus.getCurrentPhase() == BlueGreenPhases.SWITCHING_OVER) {
        throw new SQLTimeoutException(
            String.format(
                "Blue/Green Deployment switchover is still in progress after %d ms. Try %s again later.",
                BG_CONNECT_TIMEOUT.getLong(props), methodName));
      }
      LOGGER.finest(String.format(
          "Blue/Green Deployment switchover is completed. Continue with %s call.", methodName));

    } finally {
      if (telemetryContext != null) {
        telemetryContext.closeContext();
      }
    }
  }

  protected void initProvider() {
    if (provider == null) {
      providerInitLock.lock();
      try {
        if (provider == null) {
          provider = this.initProviderSupplier.get();
        }
      } finally {
        providerInitLock.unlock();
      }
    }
  }
}
