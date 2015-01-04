/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat;

import net.kuujo.copycat.protocol.Consistency;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * State log partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface StateLogPartition<T> extends ResourcePartition<StateLogPartition<T>> {

  /**
   * Registers a state command.
   *
   * @param name The command name.
   * @param command The command function.
   * @param <U> The command input type.
   * @param <V> The command output type.
   * @return The state log.
   */
  <U extends T, V> StateLogPartition<T> registerCommand(String name, Function<U, V> command);

  /**
   * Unregisters a state command.
   *
   * @param name The command name.
   * @return The state log.
   */
  StateLogPartition<T> unregisterCommand(String name);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The state log.
   */
  <U extends T, V> StateLogPartition<T> registerQuery(String name, Function<U, V> query);

  /**
   * Registers a state query.
   *
   * @param name The query name.
   * @param query The query function.
   * @param consistency The default query consistency.
   * @param <U> The query input type.
   * @param <V> The query output type.
   * @return The state log.
   */
  <U extends T, V> StateLogPartition<T> registerQuery(String name, Function<U, V> query, Consistency consistency);

  /**
   * Unregisters a state query.
   *
   * @param name The query name.
   * @return The state log.
   */
  StateLogPartition<T> unregisterQuery(String name);

  /**
   * Unregisters a state command or query.
   *
   * @param name The command or query name.
   * @return The state log.
   */
  StateLogPartition<T> unregister(String name);

  /**
   * Registers a state log snapshot function.
   *
   * @param snapshotter The snapshot function.
   * @return The state log.
   */
  <V> StateLogPartition<T> snapshotWith(Supplier<V> snapshotter);

  /**
   * Registers a state log snapshot installer.
   *
   * @param installer The snapshot installer.
   * @return The state log.
   */
  <V> StateLogPartition<T> installWith(Consumer<V> installer);

  /**
   * Submits a state command or query to the log.
   *
   * @param command The command name.
   * @param entry The command entry.
   * @param <U> The command return type.
   * @return A completable future to be completed once the command output is received.
   */
  <U> CompletableFuture<U> submit(String command, T entry);

}