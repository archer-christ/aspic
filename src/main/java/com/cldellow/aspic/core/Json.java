package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.*;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;

import java.util.Locale;
import java.util.Map;

public final class Json {
    public static final JsonCodec<FileStats> FILE_STATS_CODEC;


    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        //CATALOG_CODEC = codecFactory.mapJsonCodec(String.class, listJsonCodec(ExampleTable.class));
        FILE_STATS_CODEC = codecFactory.jsonCodec(FileStats.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                StandardTypes.BOOLEAN, BooleanType.BOOLEAN,
                StandardTypes.BIGINT, BigintType.BIGINT,
                StandardTypes.INTEGER, IntegerType.INTEGER,
                StandardTypes.DOUBLE, DoubleType.DOUBLE,
                StandardTypes.VARCHAR, VarcharType.VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(Locale.ENGLISH));
            if (type == null) {
                throw new IllegalArgumentException(String.valueOf("Unknown type " + value));
            }
            return type;
        }
    }

}
