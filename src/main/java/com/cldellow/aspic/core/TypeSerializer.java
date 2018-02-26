package com.cldellow.aspic.core;

import com.facebook.presto.spi.type.*;

public class TypeSerializer {
    private final static Type[] types = new Type[]{
            VarcharType.VARCHAR,
            BooleanType.BOOLEAN,
            DateType.DATE,
            TimestampType.TIMESTAMP,
            BigintType.BIGINT,
            IntegerType.INTEGER,
            SmallintType.SMALLINT,
            TinyintType.TINYINT,
            RealType.REAL
    };

    public static int typeToId(Type type) {
        for(int i = 0; i < types.length; i++) {
            if(types[i].equals(type))
                return i;
        }
        throw new IllegalArgumentException("unknown: " + type);
    }

    // todo: this should be in a wrapper or something to avoid
    // chasing ifs
    public static int width(Type type) {
        if(type.equals(TinyintType.TINYINT))
            return 1;
        if(type.equals(SmallintType.SMALLINT))
            return 2;
        if(type.equals(IntegerType.INTEGER))
            return 4;
        if(type.equals(BigintType.BIGINT))
            return 8;
        if(type.equals(BooleanType.BOOLEAN))
            return 1;
        if(type.equals(RealType.REAL))
            return 4;
        if(type.equals(DateType.DATE))
            return 4;
        if(type.equals(TimestampType.TIMESTAMP))
            return 8;
        if(type.equals(VarcharType.VARCHAR))
            return -1;

        throw new IllegalArgumentException("unexpected type: " + type);
    }

    public static Type idToType(int id) {
        if(id >= 0 && id < types.length)
            return types[id];
                throw new IllegalArgumentException("unexpected id " + id);
    }
}
