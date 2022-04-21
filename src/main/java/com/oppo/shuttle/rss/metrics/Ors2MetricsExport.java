/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oppo.shuttle.rss.metrics;

import com.oppo.shuttle.rss.common.OppoCloudMetric;
import com.oppo.shuttle.rss.common.Ors2MetricRegisterBean;
import com.oppo.shuttle.rss.util.JsonUtils;
import com.oppo.shuttle.rss.util.NetworkUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.DefaultEventLoop;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Ors2MetricsExport {
    private static final Logger logger = LoggerFactory.getLogger(Ors2MetricsConstants.class);

    static HTTPServer server;

    public final static String METRICS_EXPORT_PORT = "metrics.export.port";
    public final static String METRICS_REGISTER_URL = "metrics.register.url";
    public final static String METRICS_REPORT_URL = "metrics.report.url";
    public final static String METRICS_REPORT_INTERVAL = "metrics.report.interval";
    private final static String METRICS_REPORT_PSA = "metrics.report.psa";

    public final static int DEFAULT_PORT = -1;
    public final static int DEFAULT_INTERVAL = 15;
    public final static String DEFAULT_PSA = "ors2-worker";

    private final DefaultEventLoop executor = new DefaultEventLoop();

    private final String serverKey;

    private final String reportUrl;
    private final int reportInterval;
    private final String reportPsa;

    private static boolean initialized = false;

    public static synchronized void initialize(String serverKey) {
        if (!initialized) {
            Ors2MetricsConstants.register();
            DefaultExports.initialize();
            new Ors2MetricsExport(serverKey);
            initialized = true;
        }
    }

    private Ors2MetricsExport(String serverKey) {
        this.serverKey = serverKey;
        int exportPort = getInt(METRICS_EXPORT_PORT, DEFAULT_PORT);
        reportUrl = getString(METRICS_REPORT_URL, "");
        reportInterval = getInt(METRICS_REPORT_INTERVAL, DEFAULT_INTERVAL);
        reportPsa = getString(METRICS_REPORT_PSA, DEFAULT_PSA);

        try {
            if (exportPort != -1) {
                InetSocketAddress socket = new InetSocketAddress("0.0.0.0", exportPort);
                server = new HTTPServer(socket, CollectorRegistry.defaultRegistry, true);
                logger.info("Metrics export success, bind port:" + server.getPort());
            }

//            register(serverKey, server.getPort());

            scheduleReport();
        } catch (Exception e) {
            logger.warn("Metrics export or register fail", e);
        }
    }

    private int getInt(String key, int dfu) {
        String value = System.getProperty(key);
        if (!StringUtils.isEmpty(value)) {
           return Integer.parseInt(value);
        } else {
            return dfu;
        }
    }

    private String getString(String key, String dfu) {
        String value = System.getProperty(key);
        if (!StringUtils.isEmpty(value)) {
            return value;
        } else {
            return dfu;
        }
    }



    public static double toMb(double size) {
        return Math.round((size / (1 << 20)) * 10) / 10D;
    }

    public void scheduleReport() {
        if (StringUtils.isEmpty(reportUrl)) {
            logger.warn("Without a report address, metrics will not be automatically reported to the oppo cloud");
            return;
        }

        executor.scheduleAtFixedRate(() -> {
           try {
               report();
           } catch (Exception e) {
               logger.warn("Metrics report fail", e);
           }
        }, reportInterval, reportInterval, TimeUnit.SECONDS);
    }

    @Deprecated
    public void register(String serverKey, int port) throws Exception {
        String url = getString(METRICS_REGISTER_URL, "");
        if (StringUtils.isEmpty(reportUrl)) {
            logger.warn("Without a register address, metrics will not be automatically reported to the oppo cloud");
            return;
        }

        ObjectMapper mapper = new ObjectMapper();
        String ip = InetAddress.getLocalHost().getHostAddress();
        Ors2MetricRegisterBean bean = new Ors2MetricRegisterBean(serverKey, ip, port);
        String data = mapper.writeValueAsString(bean);
        NetworkUtils.putForString(url, data);
        logger.info("Metrics register success, request: " + data);
    }

    public void report(){
        ArrayList<OppoCloudMetric> list = new ArrayList<>();
        Enumeration<Collector.MetricFamilySamples> familySamples = CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (familySamples.hasMoreElements()) {
            List<Collector.MetricFamilySamples.Sample> samples = familySamples.nextElement().samples;
            samples.forEach(sample -> {
                OppoCloudMetric metric = new OppoCloudMetric();
                metric.setMetric(sample.name);
                metric.setInstance(serverKey);
                metric.setValue(sample.value);
                metric.setTimestamp(System.currentTimeMillis() / 1000);

                for (int i = 0; i < sample.labelNames.size(); i++) {
                    metric.getTags().put(sample.labelNames.get(i), sample.labelValues.get(i));
                }
                metric.getTags().put("appid", reportPsa);

                list.add(metric);
            });
        }
        if (list.isEmpty()) {
            return;
        }

        NetworkUtils.postForString(reportUrl, JsonUtils.objToJson(list));
    }
}
