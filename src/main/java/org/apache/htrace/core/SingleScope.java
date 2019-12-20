/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.htrace.core;

import java.io.Closeable;

public class SingleScope implements Closeable {
    /**
     * The tracer to use for this scope.
     */
    final Tracer tracer;

    /**
     * The trace span for this scope, or null if the scope is closed.
     * <p>
     * If the scope is closed, it must also be detached.
     */
    private final Span span;

    /**
     * The parent of this trace scope, or null if there is no parent.
     */
    private TraceScope parent;

    /**
     * True if this scope is detached.
     */
    boolean detached;

    SingleScope(Tracer tracer, Span span, TraceScope parent) {
        this.tracer = tracer;
        this.span = span;
        this.parent = parent;
        this.detached = false;
    }

    public Span getSpan() {
        return span;
    }

    public Tracer getTracer() {
        return tracer;
    }

    public TraceScope getParent() {
        return parent;
    }

    public void setParent(TraceScope parent) {
        this.parent = parent;
    }

    public boolean isDetached() {
        return detached;
    }

    public void setDetached(boolean detached) {
        this.detached = detached;
    }

    public void addTimelineAnnotation(String msg) {
        span.addTimelineAnnotation(msg);
    }

    @Override
    public void close() {
        tracer.closeTestScope(this);
    }

    @Override
    public String toString() {
        return "testScope{" +
                "tracer=" + tracer +
                ", span=" + span +
                ", parent=" + parent +
                ", detached=" + detached +
                '}';
    }
}
