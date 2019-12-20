package net.qihoo.jaeger.util;

import net.qihoo.jaeger.thrift.thriftjava.SpanRef;
import net.qihoo.jaeger.thrift.thriftjava.Tag;
import net.qihoo.jaeger.thrift.thriftjava.TagType;
import net.qihoo.jaeger.thrift.thriftjava.ThriftSpan;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.core.Span;

import java.util.*;

/**
 * This class transforms a HTrace:Span object to a Thrift:Span object.
 * we need to store the Span information in a Thrift specific format.
 */
public class HTraceToThriftConverter {
    private static final Log LOG = LogFactory.getLog(HTraceToThriftConverter.class);
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
     * This is how both Span objects are related:
     *
     * <p>
     * Trace Link Name:
     *      thrift:serverName <=> HTrace:TraceId
     * <p>
     * Trace Id:
     *      thrift:tracerId <=> HTrace:SpanId().getHigh()
     * <p>
     * Span Id:
     *      thrift:spanId <=> HTrace:SpanId().getLow()
     * <p>
     * Span Name:
     *      thrift:operationName <=> HTrace:Span.getDescription()
     * <p>
     * Start Time, Jaeger is measured in nanoseconds, Trace is in milliseconds:
     *      thrift:startTime <=> HTrace:Span.getStartTimeMillis() * 1000
     * <p>
     * Duration time, Jaeger is measured in nanoseconds, Trace is in milliseconds:
     *      thrift:duration <=> HTrace:Span.getAccumulatedMillis() * 1000
     * <p>
     * Parent Id:
     *      thrift:parentSpanId <=> HTrace:Span.getParents()[0].getLow()
     */
    public ThriftSpan convert(Span hTraceSpan) {
        String serverName = hTraceSpan.getTracerId().toLowerCase();
        long tracerId = hTraceSpan.getSpanId().getHigh();
        long tracerIdHigh = 0;
        long spanId = hTraceSpan.getSpanId().getLow();
        long parentSpanId = 0;
        String operationName = hTraceSpan.getDescription();
        int flags = (byte) 1;
        long startTime = hTraceSpan.getStartTimeMillis() * 1000;
        long duration = hTraceSpan.getAccumulatedMillis() * 1000;

        List<SpanRef> references = Collections.emptyList();
        List<Tag> tags = buildTags(hTraceSpan.getKVAnnotations());
        if (hTraceSpan.getParents().length == 0) {
            Map<String, Object> tag = new HashMap<String, Object>();
            tag.put("sampler.type", "const");
            tag.put("sampler.param", true);
            tags = buildTags(tag);
        }
        if (hTraceSpan.getParents().length > 0) {
            parentSpanId = hTraceSpan.getParents()[0].getLow();
        }

        ThriftSpan thriftSpan = new ThriftSpan(tracerId, tracerIdHigh, spanId, parentSpanId, operationName, flags, startTime, duration)
                .setReferences(references).setTags(tags).setLogs(null);
        thriftSpan.setServerName(serverName);
        return thriftSpan;
    }
}