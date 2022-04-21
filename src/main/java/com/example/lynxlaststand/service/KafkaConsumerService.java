package com.example.lynxlaststand.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import com.example.lynxlaststand.repository.ClientRepository;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


import com.example.lynxlaststand.model.Client;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.set;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.addFields;

@Service
public class KafkaConsumerService {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @Autowired
    private ClientRepository clientRepository;


    @KafkaListener(topics = "client-2-log", groupId = "group-id-2", containerFactory = "clientKafkaListenerContainerFactory")
    public void consume(Client curatedClient) {

        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");


        logger.info(String.format("Client create -> %s", curatedClient));

        System.out.println(curatedClient.isAtRisk());

        if (curatedClient.getId() != 0) {
            MongoDatabase updateClient = (MongoDatabase) mongoClient.getDatabase("allSingleClientsCuratedTest");
            MongoCollection clientCollection = (MongoCollection) updateClient.getCollection("curatedClient");

            //find ID
            Bson idQuery = eq("_id", curatedClient.getId());
            System.out.print(clientCollection);
            Bson newFields = set("Gambling", curatedClient.isGambling());
            Bson newFields2 = set("Crypto", curatedClient.isCrypto());
            UpdateOptions options = new UpdateOptions().upsert(false);

            //Only update the 1st database "atRisk" field if the second database atRisk = true
            if (curatedClient.isAtRisk()) {

                Bson newFields3 = set("atRisk", curatedClient.isAtRisk());
                UpdateResult updateResult3 = clientCollection.updateOne(idQuery, newFields3, options);
            }

            UpdateResult updateResult = clientCollection.updateOne(idQuery, newFields, options);
            UpdateResult updateResult2 = clientCollection.updateOne(idQuery, newFields2, options);




        }
    }

}
