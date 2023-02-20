package com.flink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
// import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.esotericsoftware.kryo.io.Input;
import com.google.gson.Gson;

public class DeserializationSchemaInputMessage implements DeserializationSchema<InputMessage> {
    private static final Gson gson = new Gson();

    @Override
    public TypeInformation<InputMessage> getProducedType() {
        return TypeInformation.of(InputMessage.class);
    }

    @Override
    public InputMessage deserialize(byte[] arg0) throws IOException {
        InputMessage res = gson.fromJson(new String(arg0), InputMessage.class);
        return res;
    }

    @Override
    public boolean isEndOfStream(InputMessage arg0) {
        return false;
    }
    
}
