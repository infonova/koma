package com.infonova.opss.koma.constants;

import com.infonova.opss.koma.reset.*;

import java.util.Map;
import java.util.HashMap;

public enum ResetType {
    BEGINNING("beginning", new BeginningReset()),
    END("end", new EndReset()),
    OFFSET("offset", new OffsetReset()),
    TIMESTAMP("timestamp", new TimestampReset());

    private Reset reset;
    private String name;

    private ResetType(String n, Reset r) {
        Holder.MAP.put(n, this);
        this.name = n;
        this.reset = r;
    }

    private static class Holder {
        static Map<String, ResetType> MAP = new HashMap<>();
    }

    public static ResetType find(String value) {
        ResetType rt = Holder.MAP.get(value);
        return rt;    
    }

    public Reset getReset() {
        return reset;
    }
    
    @Override
    public String toString() {
        return name;
    }
}

