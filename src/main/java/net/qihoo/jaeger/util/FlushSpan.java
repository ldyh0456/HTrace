package net.qihoo.jaeger.util;

import org.apache.htrace.core.Span;
import org.apache.htrace.core.SpanId;
import org.apache.htrace.core.TimelineAnnotation;

import java.util.List;
import java.util.Map;

public class FlushSpan implements Span {
    @Override
    public void stop() {

    }

    @Override
    public long getStartTimeMillis() {
        return 0;
    }

    @Override
    public long getStopTimeMillis() {
        return 0;
    }

    @Override
    public long getAccumulatedMillis() {
        return 0;
    }

    @Override
    public boolean isRunning() {
        return false;
    }

    @Override
    public String getDescription() {
        return null;
    }

    @Override
    public SpanId getSpanId() {
        return null;
    }

    @Override
    public Span child(String description) {
        return null;
    }

    @Override
    public SpanId[] getParents() {
        return new SpanId[0];
    }

    @Override
    public void setParents(SpanId[] parents) {

    }

    @Override
    public void addKVAnnotation(String key, String value) {

    }

    @Override
    public void addTimelineAnnotation(String msg) {

    }

    @Override
    public Map<String, String> getKVAnnotations() {
        return null;
    }

    @Override
    public List<TimelineAnnotation> getTimelineAnnotations() {
        return null;
    }

    @Override
    public String getTracerId() {
        return null;
    }

    @Override
    public void setTracerId(String s) {

    }

    @Override
    public String toJson() {
        return null;
    }
}
