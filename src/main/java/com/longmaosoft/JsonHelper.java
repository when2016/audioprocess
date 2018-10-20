package com.longmaosoft;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonHelper {
    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
        mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, false);

        mapper.getSerializerProvider().setNullValueSerializer(new JsonSerializer() {
            public void serialize(Object value, JsonGenerator jg, SerializerProvider sp)
                    throws IOException {
                jg.writeString("");
            }
        });
    }

    public static Map toMap(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return (Map) mapper.readValue(json, Map.class);
        } catch (Exception localException) {
        }
        return null;
    }

    public static List toList(String json) {
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return (List) mapper.readValue(json, List.class);
        } catch (Exception localException) {
        }
        return null;
    }

    public static <T> T fromString(String str, Class<T> valueType)
            throws IOException, JsonParseException, JsonMappingException {
        byte[] bytes = str.getBytes("UTF-8");
        return (T) mapper.readValue(bytes, 0, bytes.length, valueType);
    }

    public static String toString(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception localException) {
        }
        return null;
    }

    public static String toStringQuietly(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (Exception e) {
        }
        return obj.toString();
    }
}
