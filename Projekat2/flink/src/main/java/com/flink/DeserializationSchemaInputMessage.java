package com.flink;

import java.io.IOException;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
