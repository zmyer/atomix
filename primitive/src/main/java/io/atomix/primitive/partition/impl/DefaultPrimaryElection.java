/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.primitive.partition.impl;

import com.google.common.collect.Sets;
import io.atomix.primitive.partition.GroupMember;
import io.atomix.primitive.partition.ManagedPrimaryElection;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryElectionService;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.session.SessionClient;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Serializer;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.atomix.primitive.operation.PrimitiveOperation.operation;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.ENTER;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.Enter;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.GET_TERM;
import static io.atomix.primitive.partition.impl.PrimaryElectorOperations.GetTerm;

/**
 * Leader elector based primary election.
 */
// TODO: 2018/7/31 by zmyer
public class DefaultPrimaryElection implements ManagedPrimaryElection {
    private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
            .register(PrimaryElectorOperations.NAMESPACE)
            .register(PrimaryElectorEvents.NAMESPACE)
            .build());

    private final PartitionId partitionId;
    private final SessionClient proxy;
    private final PrimaryElectionService service;
    private final Set<PrimaryElectionEventListener> listeners = Sets.newCopyOnWriteArraySet();
    private final PrimaryElectionEventListener eventListener;
    private final AtomicBoolean started = new AtomicBoolean();

    // TODO: 2018/7/31 by zmyer
    public DefaultPrimaryElection(PartitionId partitionId, SessionClient proxy, PrimaryElectionService service) {
        this.partitionId = checkNotNull(partitionId);
        this.proxy = proxy;
        this.service = service;
        this.eventListener = event -> {
            if (event.partitionId().equals(partitionId)) {
                listeners.forEach(l -> l.onEvent(event));
            }
        };
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public CompletableFuture<PrimaryTerm> enter(GroupMember member) {
        return proxy.execute(operation(ENTER, SERIALIZER.encode(new Enter(partitionId, member)))).thenApply(
                SERIALIZER::decode);
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public CompletableFuture<PrimaryTerm> getTerm() {
        return proxy.execute(operation(GET_TERM, SERIALIZER.encode(new GetTerm(partitionId)))).thenApply(
                SERIALIZER::decode);
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public synchronized void addListener(final PrimaryElectionEventListener listener) {
        listeners.add(checkNotNull(listener));
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public synchronized void removeListener(final PrimaryElectionEventListener listener) {
        listeners.remove(checkNotNull(listener));
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public CompletableFuture<PrimaryElection> start() {
        service.addListener(eventListener);
        started.set(true);
        return CompletableFuture.completedFuture(this);
    }

    @Override
    public boolean isRunning() {
        return started.get();
    }

    // TODO: 2018/7/31 by zmyer
    @Override
    public CompletableFuture<Void> stop() {
        service.removeListener(eventListener);
        started.set(false);
        return CompletableFuture.completedFuture(null);
    }
}
