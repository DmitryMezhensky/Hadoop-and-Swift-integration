package org.apache.hadoop.swift.fs.util;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;


public class JSONUtil {
    private static ObjectMapper jsonMapper = new ObjectMapper();

    /**
     * Private constructor.
     */
    private JSONUtil() {
    }

    /**
     * Converting object to JSON string. If errors appears throw
     * MeshinException runtime exception.
     *
     * @param object The object to convert.
     * @return The JSON string representation.
     */
    public static String toJSON(Object object) {
        Writer json = new StringWriter();
        try {
            jsonMapper.writeValue(json, object);
            return json.toString();
        } catch (JsonGenerationException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (IOException e) {
            throw new RuntimeException("Error generating response", e);
        }
    }

    /**
     * Convert string representation to object. If errors appears throw
     * Exception runtime exception.
     *
     * @param value The JSON string.
     * @param klazz The class to convert.
     * @return The Object of the given class.
     */
    public static <T> T toObject(String value, Class<T> klazz) {
        try {
            return jsonMapper.readValue(value, klazz);
        } catch (JsonGenerationException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (IOException e) {
            throw new RuntimeException("Error generating response", e);
        }
    }

    /**
     *
     * @param value json string
     * @param typeReference class type reference
     * @param <T> type
     * @return deserialized  T object
     */
    public static <T> T toObject(String value, final TypeReference<T> typeReference) {
        try {
            return jsonMapper.readValue(value, typeReference);
        } catch (JsonGenerationException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Error generating response", e);
        } catch (IOException e) {
            throw new RuntimeException("Error generating response", e);
        }
    }
}
