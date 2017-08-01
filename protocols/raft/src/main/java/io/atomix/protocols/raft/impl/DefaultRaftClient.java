/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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
package io.atomix.protocols.raft.impl;

import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftMetadataClient;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.proxy.RecoveryStrategy;
import io.atomix.protocols.raft.proxy.impl.BlockingAwareRaftProxyClient;
import io.atomix.protocols.raft.proxy.impl.DelegatingRaftProxy;
import io.atomix.protocols.raft.proxy.impl.MemberSelectorManager;
import io.atomix.protocols.raft.proxy.impl.RaftProxyManager;
import io.atomix.protocols.raft.proxy.impl.RecoveringRaftProxyClient;
import io.atomix.protocols.raft.proxy.impl.RetryingRaftProxyClient;
import io.atomix.utils.concurrent.ThreadPoolContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.utils.concurrent.Threads.namedThreads;

/**
 * Default Raft client implementation.
 */
public class DefaultRaftClient implements RaftClient {
  private final String clientId;
  private final Collection<MemberId> cluster;
  private final ScheduledExecutorService threadPoolExecutor;
  private final RaftMetadataClient metadata;
  private final MemberSelectorManager selectorManager = new MemberSelectorManager();
  private final RaftProxyManager sessionManager;

  public DefaultRaftClient(
      String clientId,
      MemberId nodeId,
      Collection<MemberId> cluster,
      RaftClientProtocol protocol,
      ScheduledExecutorService threadPoolExecutor) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.cluster = checkNotNull(cluster, "cluster cannot be null");
    this.threadPoolExecutor = checkNotNull(threadPoolExecutor, "threadPoolExecutor cannot be null");
    this.metadata = new DefaultRaftMetadataClient(clientId, protocol, selectorManager, new ThreadPoolContext(threadPoolExecutor));
    this.sessionManager = new RaftProxyManager(clientId, nodeId, protocol, selectorManager, threadPoolExecutor);
  }

  @Override
  public String clientId() {
    return clientId;
  }

  @Override
  public RaftMetadataClient metadata() {
    return metadata;
  }

  @Override
  public synchronized CompletableFuture<RaftClient> connect(Collection<MemberId> cluster) {
    CompletableFuture<RaftClient> future = new CompletableFuture<>();

    // If the provided cluster list is null or empty, use the default list.
    if (cluster == null || cluster.isEmpty()) {
      cluster = this.cluster;
    }

    // If the default list is null or empty, use the default host:port.
    if (cluster == null || cluster.isEmpty()) {
      throw new IllegalArgumentException("No cluster specified");
    }

    // Reset the connection list to allow the selection strategy to prioritize connections.
    sessionManager.resetConnections(null, cluster);

    // Register the session manager.
    sessionManager.open().whenCompleteAsync((result, error) -> {
      if (error == null) {
        future.complete(this);
      } else {
        future.completeExceptionally(error);
      }
    }, threadPoolExecutor);
    return future;
  }

  @Override
  public RaftProxy.Builder newProxyBuilder() {
    return new SessionBuilder();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return sessionManager.close().thenRunAsync(threadPoolExecutor::shutdownNow);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", clientId)
        .toString();
  }

  /**
   * Default Raft session builder.
   */
  private class SessionBuilder extends RaftProxy.Builder {
    @Override
    public RaftProxy build() {
      // Create a client builder that uses the session manager to open a session.
      RaftProxyClient.Builder clientBuilder = new RaftProxyClient.Builder() {
        @Override
        public CompletableFuture<RaftProxyClient> buildAsync() {
          return sessionManager.openSession(name, serviceType, readConsistency, communicationStrategy, timeout);
        }
      };

      // Populate the proxy client builder.
      clientBuilder.withName(name)
          .withServiceType(serviceType)
          .withReadConsistency(readConsistency)
          .withMaxRetries(maxRetries)
          .withRetryDelay(retryDelay)
          .withCommunicationStrategy(communicationStrategy)
          .withRecoveryStrategy(recoveryStrategy)
          .withTimeout(timeout);

      RaftProxyClient client;

      // If the recovery strategy is set to RECOVER, wrap the builder in a recovering proxy client.
      if (recoveryStrategy == RecoveryStrategy.RECOVER) {
        client = new RecoveringRaftProxyClient(clientId, name, serviceType, clientBuilder, new ThreadPoolContext(threadPoolExecutor));
      } else {
        client = clientBuilder.build();
      }

      // If max retries is set, wrap the client in a retrying proxy client.
      if (maxRetries > 0) {
        client = new RetryingRaftProxyClient(client, new ThreadPoolContext(threadPoolExecutor), maxRetries, retryDelay);
      }

      // Default the executor to use the configured thread pool executor and create a blocking aware proxy client.
      Executor executor = this.executor != null ? this.executor : new ThreadPoolContext(threadPoolExecutor);
      client = new BlockingAwareRaftProxyClient(client, executor);

      // Create the proxy.
      return new DelegatingRaftProxy(client);
    }
  }

  /**
   * Default Raft client builder.
   */
  public static class Builder extends RaftClient.Builder {
    public Builder(Collection<MemberId> cluster) {
      super(cluster);
    }

    @Override
    public RaftClient build() {
      checkNotNull(nodeId, "nodeId cannot be null");
      Logger log = ContextualLoggerFactory.getLogger(DefaultRaftClient.class, LoggerContext.builder(RaftClient.class)
          .addValue(clientId)
          .build());
      ThreadFactory threadFactory = namedThreads("raft-client-" + clientId + "-%d", log);
      ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
      return new DefaultRaftClient(clientId, nodeId, cluster, protocol, executor);
    }
  }
}
