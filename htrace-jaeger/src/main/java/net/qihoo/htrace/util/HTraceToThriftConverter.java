package net.qihoo.htrace.util;

import jaeger.thrift.thriftjava.Span;
import jaeger.thrift.thriftjava.SpanRef;
import jaeger.thrift.thriftjava.SpanRefType;
import jaeger.thrift.thriftjava.Tag;
import jaeger.thrift.thriftjava.TagType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HTraceToThriftConverter {
    private static List<SpanRef> thriftReferences(long tracerid, long spanId) {
        List<SpanRef> thriftReferences = new ArrayList<SpanRef>();
        SpanRefType thriftRefType = SpanRefType.CHILD_OF;
        thriftReferences.add(new SpanRef(thriftRefType, tracerid, 0, spanId));
        return thriftReferences;
    }

    private static List<Tag> buildTags(Map<String, ?> tags) {
        List<Tag> thriftTags = new ArrayList<Tag>();
        if (tags != null) {
            for (Map.Entry<String, ?> entry : tags.entrySet()) {
                String tagKey = entry.getKey();
                Object tagValue = entry.getValue();
                thriftTags.add(buildTag(tagKey, tagValue));
            }
        }
        return thriftTags;
    }

    private static Tag buildTag(String tagKey, Object tagValue) {
        Tag tag = new Tag();
        tag.setKey(tagKey);
        if (tagValue instanceof Integer || tagValue instanceof Short || tagValue instanceof Long) {
            tag.setVType(TagType.LONG);
            tag.setVLong(((Number) tagValue).longValue());
        } else if (tagValue instanceof Double || tagValue instanceof Float) {
            tag.setVType(TagType.DOUBLE);
            tag.setVDouble(((Number) tagValue).doubleValue());
        } else if (tagValue instanceof Boolean) {
            tag.setVType(TagType.BOOL);
            tag.setVBool((Boolean) tagValue);
        } else {
            tag.setVType(TagType.STRING);
            tag.setVStr(String.valueOf(tagValue));
        }
        return tag;
    }

    /**
     * This is HTraceSpan transition thriftSpan
     *
     */
    public Span convert(org.apache.htrace.core.Span hTraceSpan) {
        String serverName = hTraceSpan.getTracerId();
        long tracerid = hTraceSpan.getSpanId().getHigh();
        long traceridHigh = 0;
        long spanId = hTraceSpan.getSpanId().getLow();
        long parentSpanId = 0;
        java.lang.String operationName = hTraceSpan.getDescription();
        int flags = (byte) 1;
        long startTime = hTraceSpan.getStartTimeMillis() * 1000;
        long duration = hTraceSpan.getAccumulatedMillis() * 1000;

        List<SpanRef> references = Collections.emptyList();
        List<Tag> tags = buildTags(hTraceSpan.getKVAnnotations());
        if (hTraceSpan.getParents().length > 0) {
            parentSpanId = hTraceSpan.getParents()[0].getLow();
            references = thriftReferences(tracerid, spanId);
        }
        Span thriftSpan = new Span(tracerid, traceridHigh, spanId, parentSpanId, operationName, flags, startTime, duration).setReferences(references).setTags(tags).setLogs(null);
        thriftSpan.setServerName(serverName);
        return thriftSpan;
    }
}