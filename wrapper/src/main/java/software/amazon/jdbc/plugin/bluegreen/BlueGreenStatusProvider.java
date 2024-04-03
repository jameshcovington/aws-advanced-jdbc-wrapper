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

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.PluginService;
import software.amazon.jdbc.PropertyDefinition;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.dialect.SupportBlueGreen;
import software.amazon.jdbc.util.PropertyUtils;

public class BlueGreenStatusProvider {

  private static final Logger LOGGER = Logger.getLogger(BlueGreenStatusProvider.class.getName());
  private static final String MONITORING_PROPERTY_PREFIX = "blue-green-monitoring-";
  private static final String DEFAULT_CONNECT_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));
  private static final String DEFAULT_SOCKET_TIMEOUT_MS = String.valueOf(TimeUnit.SECONDS.toMillis(10));

  protected static BlueGreenStatusMonitor greenBlueGreenStatusMonitor = null;
  protected static final ReentrantLock monitorInitLock = new ReentrantLock();

  //TODO: make it configurable
  protected static final HashMap<BlueGreenPhases, Long> defaultCheckIntervalMap =
      new HashMap<BlueGreenPhases, Long>() {
        {
          put(BlueGreenPhases.NOT_CREATED, TimeUnit.MINUTES.toMillis(5));
          //put(BlueGreenPhases.CREATED, TimeUnit.SECONDS.toMillis(30));
          put(BlueGreenPhases.CREATED, TimeUnit.SECONDS.toMillis(5)); // TODO: debug
          put(BlueGreenPhases.SWITCHING_OVER, TimeUnit.SECONDS.toMillis(1));
          put(BlueGreenPhases.SWITCH_OVER_COMPLETED, TimeUnit.SECONDS.toMillis(30));
        }
      };

  protected final PluginService pluginService;
  protected final Properties props;

  public BlueGreenStatusProvider(
      final @NonNull PluginService pluginService,
      final @NonNull Properties props) {

    this.pluginService = pluginService;
    this.props = props;

    final Dialect dialect = this.pluginService.getDialect();
    if (dialect instanceof SupportBlueGreen) {
      final SupportBlueGreen blueGreenDialect = (SupportBlueGreen) dialect;
      this.initMonitoring(blueGreenDialect.getBlueGreenStatusQuery());
    } else {
      LOGGER.warning("Blue/Green Deployments isn't supported by this database engine.");
    }
  }

  protected void initMonitoring(final String blueGreenStatusQuery) {
    if (greenBlueGreenStatusMonitor == null) {
      monitorInitLock.lock();
      try {
        if (greenBlueGreenStatusMonitor == null) {
          greenBlueGreenStatusMonitor =
              new BlueGreenStatusMonitor(
                  this.pluginService,
                  this.getMonitoringProperties(),
                  blueGreenStatusQuery,
                  defaultCheckIntervalMap);
        }
      } finally {
        monitorInitLock.unlock();
      }
    }
  }

  protected Properties getMonitoringProperties() {
    final Properties monitoringConnProperties = PropertyUtils.copyProperties(this.props);
    this.props.stringPropertyNames().stream()
        .filter(p -> p.startsWith(MONITORING_PROPERTY_PREFIX))
        .forEach(
            p -> {
              monitoringConnProperties.put(
                  p.substring(MONITORING_PROPERTY_PREFIX.length()),
                  this.props.getProperty(p));
              monitoringConnProperties.remove(p);
            });

    if (!monitoringConnProperties.containsKey(PropertyDefinition.CONNECT_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.CONNECT_TIMEOUT.name, DEFAULT_CONNECT_TIMEOUT_MS);
    }
    if (!monitoringConnProperties.containsKey(PropertyDefinition.SOCKET_TIMEOUT)) {
      monitoringConnProperties.setProperty(PropertyDefinition.SOCKET_TIMEOUT.name, DEFAULT_SOCKET_TIMEOUT_MS);
    }

    return monitoringConnProperties;
  }
}
