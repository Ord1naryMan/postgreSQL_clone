package org.ord1naryman.postgresClone;

import org.ord1naryman.postgresClone.model.Database;
import org.ord1naryman.postgresClone.operations.Insert;
import org.ord1naryman.postgresClone.operations.Select;
import org.ord1naryman.postgresClone.operations.SelectFrom;

import java.util.HashMap;
import java.util.Map;

public class Demo {


    //id : int
    // model : string
    // type : int
    // capacity_kg : int
    // lifting_capacity_kg : int
    // max_flight_range_km : int

    static Map<String, Class<?>> airportStructure = new HashMap<>(
        //I know that by SQL convention I should use snake case, but I don't use SQL :)
        Map.of(
            "id", Integer.class,
            "model", String.class,
            "typeId", Integer.class,
            "peopleCapacity", Integer.class,
            "liftingCapacityKg", Integer.class,
            "maxFlightRangeKm", Integer.class
        )
    );

    static Map<String, Class<?>> planeTypeStructure = new HashMap<>(
        Map.of(
            "id", Integer.class,
            "typeName", String.class
        )
    );

    public static void main(String[] args) {
        var database = new Database("airport");
        var airport = database.createTable("airport", airportStructure);
        var planeType = database.createTable("planeType", planeTypeStructure);
        Insert.into(planeType)
            .value(Map.of("id", 1, "typeName", "passengerAircraft"))
            .value(Map.of("id", 2, "typeName", "cargoLiftingAircraft"))
            .value(Map.of("id", 3, "typeName", "farmerAircraft"));
        Insert.into(airport)
            .value(Map.of(
                "id", 1,
                "model", "Boeing",
                "typeId", 1,
                "peopleCapacity", 100,
                "liftingCapacityKg", 20000,
                "maxFlightRangeKm", 100
            ))
            .value(Map.of(
                "id", 2,
                "model", "Boeing",
                "typeId", 2,
                "peopleCapacity", 0,
                "liftingCapacityKg", 100000,
                "maxFlightRangeKm", 200
            ))
            .value(Map.of(
                "id", 3,
                "model", "AirCraftInc",
                "typeId", 3,
                "peopleCapacity", 2,
                "liftingCapacityKg", 500,
                "maxFlightRangeKm", 50
            ))
            .value(Map.of(
                "id", 4,
                "model", "Belavia",
                "typeId", 1,
                "peopleCapacity", 200,
                "liftingCapacityKg", 30000,
                "maxFlightRangeKm", 150
            ))
            .value(Map.of(
                "id", 5,
                "model", "Belavia",
                "typeId", 2,
                "peopleCapacity", 0,
                "liftingCapacityKg", 500000,
                "maxFlightRangeKm", 300
            ))
            .value(Map.of(
                "id", 6,
                "model", "TopAircrafts",
                "typeId", 3,
                "peopleCapacity", 2,
                "liftingCapacityKg", 3000,
                "maxFlightRangeKm", 80
            ));

        var liftingCapacity = Select.from(airport)
            .sumBy("liftingCapacityKg");

        var peopleCapacity = Select.from(airport)
            .sumBy("peopleCapacity");

        System.out.printf("Total lifting capacity of our airport is %s" + System.lineSeparator(), liftingCapacity);
        System.out.printf("Total people capacity of our airport is %s" + System.lineSeparator(), peopleCapacity);

        var aircraftsSorted = Select.from(airport)
            .orderBy("maxFlightRangeKm")
            .join(Select.from(planeType))
            .on("typeId", "id")
            .execute();

        System.out.println("Here are our aircrafts, sorted by maxFlightRangeKm: ");
        System.out.println(aircraftsSorted);

        var aircraftsInRange = Select.from(airport).whereBetween("maxFlightRangeKm", 100, 150).execute();
        System.out.println("Here are aircrafts which max flight range is between 100km and 150km");
        System.out.println(aircraftsInRange);
    }
}
