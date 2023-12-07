package org.ord1naryman.postgresClone.core;

import org.ord1naryman.postgresClone.model.Table;

import java.util.HashMap;
import java.util.Map;

public class ConnectionPool {
    public static Map<String ,Table> openConnections = new HashMap<>();
}

