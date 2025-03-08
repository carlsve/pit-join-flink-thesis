package com.carlsve.app.pit_stream;

import java.io.Serializable;

public class RowPojo implements Serializable {
    public String id;
    public Integer ts;
    public String val;

    public RowPojo() {}
    public RowPojo(String id, Integer ts, String val) {
        this.id = id;
        this.ts = ts;
        this.val = val;
    }

    public String stringify() {
        return "{id:" + id + ",ts:" + ts + ",val:" + val + "}";
    }
}
