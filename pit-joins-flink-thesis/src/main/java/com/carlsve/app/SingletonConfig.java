package com.carlsve.app;

import java.util.HashMap;

public class SingletonConfig extends HashMap<String, String> {
    private static SingletonConfig single_instance = null;

    public static synchronized SingletonConfig getInstance() {
        if (single_instance == null) {
            single_instance = new SingletonConfig();
        }

        return single_instance;
    }    
}
