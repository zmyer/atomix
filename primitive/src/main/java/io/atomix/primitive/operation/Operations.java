/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.primitive.operation;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Operation utilities.
 */
// TODO: 2018/8/1 by zmyer
public final class Operations {

    /**
     * Returns the collection of operations provided by the given service interface.
     *
     * @param serviceInterface the service interface
     * @return the operations provided by the given service interface
     */
    // TODO: 2018/8/1 by zmyer
    public static Map<Method, OperationId> getMethodMap(final Class<?> serviceInterface) {
        if (!serviceInterface.isInterface()) {
            final Map<Method, OperationId> operations = new HashMap<>();
            for (final Class<?> iface : serviceInterface.getInterfaces()) {
                operations.putAll(findMethods(iface));
            }
            return operations;
        }
        return findMethods(serviceInterface);
    }

    /**
     * Recursively finds operations defined by the given type and its implemented interfaces.
     *
     * @param type the type for which to find operations
     * @return the operations defined by the given type and its parent interfaces
     */
    // TODO: 2018/8/1 by zmyer
    private static Map<Method, OperationId> findMethods(final Class<?> type) {
        final Map<Method, OperationId> operations = new HashMap<>();
        for (final Method method : type.getDeclaredMethods()) {
            final OperationId operationId = getOperationId(method);
            if (operationId != null) {
                if (operations.values().stream().anyMatch(operation -> operation.id().equals(operationId.id()))) {
                    throw new IllegalStateException("Duplicate operation name '" + operationId.id() + "'");
                }
                operations.put(method, operationId);
            }
        }
        for (Class<?> iface : type.getInterfaces()) {
            operations.putAll(findMethods(iface));
        }
        return operations;
    }

    /**
     * Returns the collection of operations provided by the given service interface.
     *
     * @param serviceInterface the service interface
     * @return the operations provided by the given service interface
     */
    public static Map<OperationId, Method> getOperationMap(Class<?> serviceInterface) {
        if (!serviceInterface.isInterface()) {
            Class type = serviceInterface;
            Map<OperationId, Method> operations = new HashMap<>();
            while (type != Object.class) {
                for (Class<?> iface : type.getInterfaces()) {
                    operations.putAll(findOperations(iface));
                }
                type = type.getSuperclass();
            }
            return operations;
        }
        return findOperations(serviceInterface);
    }

    /**
     * Recursively finds operations defined by the given type and its implemented interfaces.
     *
     * @param type the type for which to find operations
     * @return the operations defined by the given type and its parent interfaces
     */
    private static Map<OperationId, Method> findOperations(Class<?> type) {
        Map<OperationId, Method> operations = new HashMap<>();
        for (Method method : type.getDeclaredMethods()) {
            OperationId operationId = getOperationId(method);
            if (operationId != null) {
                if (operations.keySet().stream().anyMatch(operation -> operation.id().equals(operationId.id()))) {
                    throw new IllegalStateException("Duplicate operation name '" + operationId.id() + "'");
                }
                operations.put(operationId, method);
            }
        }
        for (Class<?> iface : type.getInterfaces()) {
            operations.putAll(findOperations(iface));
        }
        return operations;
    }

    /**
     * Returns the operation ID for the given method.
     *
     * @param method the method for which to lookup the operation ID
     * @return the operation ID for the given method or null if the method is not annotated
     */
    // TODO: 2018/8/1 by zmyer
    private static OperationId getOperationId(final Method method) {
        final Command command = method.getAnnotation(Command.class);
        if (command != null) {
            final String name = command.value().equals("") ? method.getName() : command.value();
            return OperationId.from(name, OperationType.COMMAND);
        }
        final Query query = method.getAnnotation(Query.class);
        if (query != null) {
            final String name = query.value().equals("") ? method.getName() : query.value();
            return OperationId.from(name, OperationType.QUERY);
        }
        final Operation operation = method.getAnnotation(Operation.class);
        if (operation != null) {
            final String name = operation.value().equals("") ? method.getName() : operation.value();
            return OperationId.from(name, operation.type());
        }
        return null;
    }

    private Operations() {
    }
}
