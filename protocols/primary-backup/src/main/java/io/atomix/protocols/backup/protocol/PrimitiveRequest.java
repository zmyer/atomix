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
package io.atomix.protocols.backup.protocol;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Primitive service request.
 */
// TODO: 2018/8/1 by zmyer
public abstract class PrimitiveRequest extends PrimaryBackupRequest {
    private final PrimitiveDescriptor primitive;

    public PrimitiveRequest(PrimitiveDescriptor primitive) {
        this.primitive = primitive;
    }

    public PrimitiveDescriptor primitive() {
        return primitive;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("primitive", primitive)
                .toString();
    }
}
