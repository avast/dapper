package com.avast.dapper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Strings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import javax.net.ssl.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterBuilder {

    protected static final Config defaultConfig = ConfigFactory.defaultReference().getConfig("utilsDatastaxDefaults");

    public static Cluster.Builder fromConfig(Config config) {
        config = config.withFallback(defaultConfig);

        final String clusterName = config.getString("clusterName");

        Cluster.Builder builder = new Cluster.Builder()
                .withPort(config.getInt("port"))
                .withProtocolVersion(ProtocolVersion.fromInt(config.getInt("protocolVersion")))
                .withCompression(parseCompression(config.getString("compression")))
                .withMaxSchemaAgreementWaitSeconds((int) config.getDuration("maxSchemaAgreementWaitSeconds", TimeUnit.SECONDS))
                .withClusterName(Strings.emptyToNull(clusterName))
                .withLoadBalancingPolicy(parseLoadBalancingPolicy(config.getConfig("loadBalancingPolicy")))
                .withPoolingOptions(parsePoolingOptions(config.getConfig("poolingOptions")))
                .withQueryOptions(parseQueryOptions(config.getConfig("queryOptions")))
                .withReconnectionPolicy(parseReconnectionPolicy(config.getConfig("reconnectionPolicy")))
                .withRetryPolicy(parseRetryPolicy(config.getConfig("retryPolicy")))
                .withSocketOptions(parseSocketOptions(config.getConfig("socketOptions")))
                .withSpeculativeExecutionPolicy(parseSpeculativeExecutionPolicy(config.getConfig("speculativeExecutionPolicy")))
                .withTimestampGenerator(parseTimestampGenerator(config.getConfig("timestampGenerator")));

        LatencyTracker latencyTracker = null;
        Host.StateListener stateListener = null;

        List<String> seeds = config.getStringList("seeds");
        builder = builder.addContactPoints(seeds.toArray(new String[seeds.size()]));

        Config sslConfig = config.getConfig("ssl");
        if (sslConfig.getBoolean("enabled")) {
            builder.withSSL(parseSSLOptions(sslConfig));
        }

        Config credentialsConfig = config.getConfig("credentials");
        if (credentialsConfig.getBoolean("enabled")) {
            builder.withCredentials(credentialsConfig.getString("username"), credentialsConfig.getString("password"));
        }
        return builder;
    }


    protected static ProtocolOptions.Compression parseCompression(String s) {
        return Arrays.stream(ProtocolOptions.Compression.values())
                .filter(v -> v.name().equalsIgnoreCase(s))
                .findFirst()
                .orElseThrow(() -> new ConfigException.BadValue("compression", "Unknown value: " + s));
    }


    protected static LoadBalancingPolicy parseLoadBalancingPolicy(Config config) {
        LoadBalancingPolicy policy = parseCoreLoadBalancingPolicy(config.getConfig("corePolicy"));
        for (String chainingPolicyName : config.getStringList("chainingPolicies")) {
            Config chainingConfig = config.getConfig("knownPolicies").getConfig(chainingPolicyName);
            policy = parseChainingLoadBalancingPolicy(policy, chainingConfig);
        }
        return policy;
    }

    protected static LoadBalancingPolicy parseCoreLoadBalancingPolicy(Config config) {
        String type = config.getString("type");
        if (type.equalsIgnoreCase("RoundRobin")) {
            return new RoundRobinPolicy();
        }
        if (type.equalsIgnoreCase("DCAwareRoundRobin")) {
            DCAwareRoundRobinPolicy.Builder builder = DCAwareRoundRobinPolicy.builder();
            builder = builder.withUsedHostsPerRemoteDc(config.getInt("usedHostsPerRemoteDc"));
            String localDC = config.getString("localDataCenter");
            if (!Strings.isNullOrEmpty(localDC)) {
                builder = builder.withLocalDc(localDC);
            }
            if (config.getBoolean("allowRemoteDCsForLocalConsistencyLevel")) {
                builder = builder.allowRemoteDCsForLocalConsistencyLevel();
            }
            return builder.build();
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown core load-balancing policy: " + type);
    }

    protected static LoadBalancingPolicy parseChainingLoadBalancingPolicy(LoadBalancingPolicy childPolicy, Config config) {
        String type = config.getString("type");
        if (type.equalsIgnoreCase("TokenAware")) {
            return new TokenAwarePolicy(childPolicy, config.getBoolean("shuffleReplicas"));
        }
        if (type.equalsIgnoreCase("LatencyAware")) {
            return new LatencyAwarePolicy.Builder(childPolicy)
                    .withExclusionThreshold(config.getDouble("exclusionThreshold"))
                    .withMininumMeasurements(config.getInt("minimumMeasurements"))
                    .withRetryPeriod(config.getDuration("retryPeriod", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                    .withScale(config.getDuration("scale", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                    .withUpdateRate(config.getDuration("updateRate", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                    .build();
        }
        if (type.equalsIgnoreCase("Whitelist")) {
            List<InetSocketAddress> list = new ArrayList<>();

            for (String host : config.getStringList("hosts")) {
                try {
                    String[] parts = host.split(":");
                    list.add(new InetSocketAddress(parts[0], Integer.valueOf(parts[1])));
                } catch (Exception ex) {
                    throw new ConfigException.BadValue(config.origin(), "hosts", host + " (expected for example 'localhost:9042')", ex);
                }
            }

            return new WhiteListPolicy(childPolicy, list);
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown chaining load-balancing policy: " + type);
    }


    protected static PoolingOptions parsePoolingOptions(Config config) {
        PoolingOptions res = new PoolingOptions()
                .setHeartbeatIntervalSeconds((int) config.getDuration("heartbeatInterval", TimeUnit.SECONDS))
                .setIdleTimeoutSeconds((int) config.getDuration("idleTimeout", TimeUnit.SECONDS));
        updatePoolingOptionsForDistance(res, HostDistance.LOCAL, config.getConfig("local"));
        updatePoolingOptionsForDistance(res, HostDistance.REMOTE, config.getConfig("remote"));
        return res;
    }

    protected static void updatePoolingOptionsForDistance(PoolingOptions options, HostDistance distance, Config config) {
        if (config.hasPath("coreConnectionsPerHost")) options.setCoreConnectionsPerHost(distance, config.getInt("coreConnectionsPerHost"));
        if (config.hasPath("maxConnectionsPerHost")) options.setMaxConnectionsPerHost(distance, config.getInt("maxConnectionsPerHost"));
        if (config.hasPath("maxRequestsPerConnection"))
            options.setMaxRequestsPerConnection(distance, config.getInt("maxRequestsPerConnection"));
        if (config.hasPath("newConnectionThreshold")) options.setNewConnectionThreshold(distance, config.getInt("newConnectionThreshold"));
    }

    protected static QueryOptions parseQueryOptions(Config config) {
        return new QueryOptions()
                .setConsistencyLevel(parseConsistencyLevel(config.getString("consistencyLevel")))
                .setSerialConsistencyLevel(parseConsistencyLevel(config.getString("serialConsistencyLevel")))
                .setDefaultIdempotence(config.getBoolean("defaultIdempotence"))
                .setFetchSize(config.getInt("fetchSize"))
                .setMaxPendingRefreshNodeListRequests(config.getInt("maxPendingRefreshNodeListRequests"))
                .setMaxPendingRefreshNodeRequests(config.getInt("maxPendingRefreshNodeRequests"))
                .setMaxPendingRefreshSchemaRequests(config.getInt("maxPendingRefreshSchemaRequests"))
                .setMetadataEnabled(config.getBoolean("metadata"))
                .setPrepareOnAllHosts(config.getBoolean("prepareOnAllHosts"))
                .setRefreshNodeIntervalMillis((int) config.getDuration("refreshNodeInterval", TimeUnit.MILLISECONDS))
                .setRefreshNodeListIntervalMillis((int) config.getDuration("refreshNodeListInterval", TimeUnit.MILLISECONDS))
                .setRefreshSchemaIntervalMillis((int) config.getDuration("refreshSchemaInterval", TimeUnit.MILLISECONDS))
                .setReprepareOnUp(config.getBoolean("reprepareOnUp"));
    }

    protected static ConsistencyLevel parseConsistencyLevel(String s) {
        return Arrays.stream(ConsistencyLevel.values())
                .filter(v -> v.name().equalsIgnoreCase(s))
                .findFirst()
                .orElseThrow(() -> new ConfigException.BadValue("consistencyLevel", "Unknown value: " + s));
    }


    protected static ReconnectionPolicy parseReconnectionPolicy(Config config) {
        String type = config.getString("type");
        if (type.equalsIgnoreCase("Constant")) {
            return new ConstantReconnectionPolicy(config.getDuration("constantDelay", TimeUnit.MILLISECONDS));
        }
        if (type.equalsIgnoreCase("Exponential")) {
            return new ExponentialReconnectionPolicy(config.getDuration("baseDelay", TimeUnit.MILLISECONDS), config.getDuration("maxDelay", TimeUnit.MILLISECONDS));
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown reconnection policy: " + type);
    }


    protected static RetryPolicy parseRetryPolicy(Config config) {
        String type = config.getString("type");
        RetryPolicy core;
        if (type.equalsIgnoreCase("Default")) {
            core = DefaultRetryPolicy.INSTANCE;
        } else if (type.equalsIgnoreCase("DowngradingConsistency")) {
            core = DowngradingConsistencyRetryPolicy.INSTANCE;
        } else if (type.equalsIgnoreCase("Fallthrough")) {
            core = FallthroughRetryPolicy.INSTANCE;
        } else {
            throw new ConfigException.BadValue(config.origin(), "type", "Unknown retry policy: " + type);
        }
        return config.getBoolean("logging") ? new LoggingRetryPolicy(core) : core;
    }

    protected static SocketOptions parseSocketOptions(Config config) {
        SocketOptions res = new SocketOptions()
                .setConnectTimeoutMillis((int) config.getDuration("connectTimeout", TimeUnit.MILLISECONDS))
                .setReadTimeoutMillis((int) config.getDuration("readTimeout", TimeUnit.MILLISECONDS));
        if (config.hasPath("keepAlive")) res.setKeepAlive(config.getBoolean("keepAlive"));
        if (config.hasPath("reuseAddress")) res.setReuseAddress(config.getBoolean("reuseAddress"));
        if (config.hasPath("tcpNoDelay")) res.setTcpNoDelay(config.getBoolean("tcpNoDelay"));
        if (config.hasPath("receiveBufferSize")) res.setReceiveBufferSize(config.getInt("receiveBufferSize"));
        if (config.hasPath("sendBufferSize")) res.setSendBufferSize(config.getInt("sendBufferSize"));
        if (config.hasPath("soLinger")) res.setSoLinger(config.getInt("soLinger"));
        return res;
    }


    protected static SpeculativeExecutionPolicy parseSpeculativeExecutionPolicy(Config config) {
        String type = config.getString("type");
        if (type.equalsIgnoreCase("No")) {
            return NoSpeculativeExecutionPolicy.INSTANCE;
        }
        if (type.equalsIgnoreCase("Constant")) {
            return new ConstantSpeculativeExecutionPolicy(
                    config.getDuration("constantDelay", TimeUnit.MILLISECONDS),
                    config.getInt("maxSpeculativeExecutions"));
        }
        if (type.equalsIgnoreCase("PercentilePerHost")) {
            return new PercentileSpeculativeExecutionPolicy(
                    PerHostPercentileTracker.builder(config.getDuration("highestTrackableLatency", TimeUnit.MILLISECONDS))
                            .withInterval(config.getDuration("interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                            .build(),
                    config.getDouble("percentile"),
                    config.getInt("maxSpeculativeExecutions"));
        }
        if (type.equalsIgnoreCase("PercentileClusterWide")) {
            return new PercentileSpeculativeExecutionPolicy(
                    ClusterWidePercentileTracker.builder(config.getDuration("highestTrackableLatency", TimeUnit.MILLISECONDS))
                            .withInterval(config.getDuration("interval", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                            .build(),
                    config.getDouble("percentile"),
                    config.getInt("maxSpeculativeExecutions"));
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown speculative execution policy: " + type);
    }


    protected static TimestampGenerator parseTimestampGenerator(Config config) {
        String type = config.getString("type");
        if (type.equalsIgnoreCase("ServerSide")) {
            return ServerSideTimestampGenerator.INSTANCE;
        }
        if (type.equalsIgnoreCase("AtomicMonotonic")) {
            return new AtomicMonotonicTimestampGenerator();
        }
        if (type.equalsIgnoreCase("ThreadLocalMonotonic")) {
            return new ThreadLocalMonotonicTimestampGenerator();
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown timestamp generator: " + type);
    }


    protected static SSLOptions parseSSLOptions(Config config) {
        String type = config.getString("implementation");
        if (type.equalsIgnoreCase("JDK")) {
            List<String> cipherSuites = config.getStringList("cipherSuites");
            return JdkSSLOptions.builder()
                    .withSSLContext(parseSSLContext(config))
                    .withCipherSuites(cipherSuites.toArray(new String[cipherSuites.size()]))
                    .build();
        }
        if (type.equalsIgnoreCase("Netty")) {
            return new NettySSLOptions(parseSslContext(config));
        }
        throw new ConfigException.BadValue(config.origin(), "type", "Unknown SSL implementation: " + type);
    }

    protected static SSLContext parseSSLContext(Config config) {
        try {
            SSLContext ctx = SSLContext.getInstance(config.getString("protocol"));

            KeyManager[] keyManagers = null;
            TrustManager[] trustManagers = null;

            Config keyStoreConfig = config.getConfig("keyStore");
            String path = keyStoreConfig.getString("path");
            char[] password = Strings.nullToEmpty(keyStoreConfig.getString("password")).toCharArray();
            if (!Strings.isNullOrEmpty(path)) {
                KeyManagerFactory kmfactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmfactory.init(loadKeyStoreFromFileOrClassPath(path, password), password);
                keyManagers = kmfactory.getKeyManagers();
            }

            Config trustStoreConfig = config.getConfig("trustStore");
            path = trustStoreConfig.getString("path");
            password = Strings.nullToEmpty(trustStoreConfig.getString("password")).toCharArray();
            if (!Strings.isNullOrEmpty(path)) {
                TrustManagerFactory tmfactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmfactory.init(loadKeyStoreFromFileOrClassPath(path, password));
                trustManagers = tmfactory.getTrustManagers();
            }

            ctx.init(keyManagers, trustManagers, null);
            return ctx;
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static SslContext parseSslContext(Config config) {
        try {
            SslContextBuilder builder = SslContextBuilder
                    .forClient()
                    .sslProvider(SslProvider.valueOf(config.getString("provider")));
            Config trustStoreConfig = config.getConfig("trustStore");
            String path = trustStoreConfig.getString("path");
            if (!Strings.isNullOrEmpty(path)) {
                char[] password = Strings.nullToEmpty(trustStoreConfig.getString("password")).toCharArray();
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(loadKeyStoreFromFileOrClassPath(path, password));
                builder = builder.trustManager(tmf);
            }
            Config keyManagerConfig = config.getConfig("keyManager");
            String keyCertChainPath = keyManagerConfig.getString("keyCertChainPath");
            if (!Strings.isNullOrEmpty(keyCertChainPath)) {
                builder = builder.keyManager(new FileInputStream(keyCertChainPath), new FileInputStream(keyManagerConfig.getString("keyPath")));
            }
            return builder.build();
        } catch (GeneralSecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static KeyStore loadKeyStoreFromFileOrClassPath(String path, char[] password) throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
        File file = new File(path);
        if (file.isFile()) {
            return loadKeyStoreFromInputStream(new FileInputStream(path), password);
        } else {
            InputStream stream = ClusterBuilder.class.getResourceAsStream(path);
            if (stream != null) {
                return loadKeyStoreFromInputStream(stream, password);
            } else {
                throw new FileNotFoundException("Keystore not found on file system or classpath: " + path);
            }
        }
    }

    protected static KeyStore loadKeyStoreFromInputStream(InputStream stream, char[] password) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
        try {
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(stream, password);
            return keyStore;
        } finally {
            stream.close();
        }
    }
}
