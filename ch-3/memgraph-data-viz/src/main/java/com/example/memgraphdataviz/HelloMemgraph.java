package com.example.memgraphdataviz;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;

import java.util.Arrays;
import java.util.List;


public class HelloMemgraph implements AutoCloseable {
    // Driiver instance used across the application
    private final Driver driver;

    // Create a driver instance, and pass the uri, username and password if required
    public HelloMemgraph(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    // Creating indexes
    public void createIndexes() {
        List<String> indexes = Arrays.asList(
                "CREATE INDEX ON :Developer(id);",
                "CREATE INDEX ON :Technology(id);",
                "CREATE INDEX ON :Developer(name);",
                "CREATE INDEX ON :Technology(name);"
        );

        try (Session session = driver.session()) {
            for (String index : indexes) {
                session.run(index);
            }
        }
    }

    //Create nodes and relationships
    public void createNodesAndRealationships() {
        List<String> developerNodes = Arrays.asList(
                "CREATE (n:Developer {id: 1, name:'Andy'});",
                "CREATE (n:Developer {id: 2, name:'John'});",
                "CREATE (n:Developer {id: 3, name:'Michael'});"
        );

        List<String> technologyNodes = Arrays.asList(
                "CREATE (n:Technology {id: 1, name:'Memgraph', description: 'Fastest graph DB in the world!', createdAt: timestamp()})",
                "CREATE (n:Technology {id: 2, name:'Java', description: 'Java programming language ', createdAt: timestamp()})",
                "CREATE (n:Technology {id: 3, name:'Docker', description: 'Docker containerization engine', createdAt: timestamp()})",
                "CREATE (n:Technology {id: 4, name:'Kubernetes', description: 'Kubernetes container orchestration engine', createdAt: timestamp()})",
                "CREATE (n:Technology {id: 5, name:'Python', description: 'Python programming language', createdAt: timestamp()})"
        );

        List<String> relationships = Arrays.asList(
                "MATCH (a:Developer {id: 1}),(b:Technology {id: 1}) CREATE (a)-[r:LOVES]->(b);",
                "MATCH (a:Developer {id: 2}),(b:Technology {id: 3}) CREATE (a)-[r:LOVES]->(b);",
                "MATCH (a:Developer {id: 3}),(b:Technology {id: 1}) CREATE (a)-[r:LOVES]->(b);",
                "MATCH (a:Developer {id: 1}),(b:Technology {id: 5}) CREATE (a)-[r:LOVES]->(b);",
                "MATCH (a:Developer {id: 2}),(b:Technology {id: 2}) CREATE (a)-[r:LOVES]->(b);",
                "MATCH (a:Developer {id: 3}),(b:Technology {id: 4}) CREATE (a)-[r:LOVES]->(b);"
        );

        try (Session session = driver.session()) {
            for (String node : developerNodes) {
                session.run(node);
            }

            for (String node : technologyNodes) {
                session.run(node);
            }

            for (String relationship : relationships) {
                session.run(relationship);
            }
        }
    }

    // Read nodes and return as Record list
    public List<Record> readNodes() {
        String query = "MATCH (n:Technology{name: 'Memgraph'}) RETURN n;";
        try (Session session = driver.session()) {
            Result result = session.run(query);
            return result.list();
        }

    }

    public static void main(String[] args) throws Exception {
        String uri="bolt://localhost:7687";
        String username="memgraph";
        String password="memgraph";

        try (var helloMemgraph = new HelloMemgraph(uri, username, password)) {
            // Create indexes
            helloMemgraph.createIndexes();
            // Create nodes and relationships
            helloMemgraph.createNodesAndRealationships();
            // Read nodes
            var result = helloMemgraph.readNodes();
            // Process results
            for (Record node : result) {
                Node n = node.get("n").asNode();
                System.out.println(n); // Node type
                System.out.println(n.asMap()); // Node properties
                System.out.println(n.id()); // Node internal ID
                System.out.println(n.labels()); // Node labels
                System.out.println(n.get("id").asLong()); // Node user defined id property
                System.out.println(n.get("name").asString()); // Node user defined name property
                System.out.println(n.get("description").asString()); // Node user defined description property
            }
        }
    }

    @Override
    public void close() throws Exception {
        driver.close();

    }
}