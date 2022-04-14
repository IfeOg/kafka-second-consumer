package com.example.lynxlaststand.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;


import com.example.lynxlaststand.model.Client;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.push;
import static com.mongodb.client.model.Updates.set;

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

        if (curatedClient.getId() != 0){
            MongoDatabase updateClient = (MongoDatabase) mongoClient.getDatabase("curatedClient");
            MongoCollection clientCollection = (MongoCollection) updateClient.getCollection("allSingleClientsCuratedTest");

            //find ID
            Bson idQuery = eq("_id", curatedClient.getId());
            Bson newFields = set("Gambling", curatedClient.isGambling());
            UpdateOptions options = new UpdateOptions().upsert(true);
            UpdateResult updateResult = clientCollection.updateOne(idQuery, newFields, options);

            //idQuery.put("_id", curatedClient.getId());

            //append
            //BasicDBObject newFields = new BasicDBObject();
            //newFields.put("Gambling", curatedClient.isGambling());
            //newFields.append("Crypto", curatedClient.isCrypto());
           // newFields.append("At Risk", curatedClient.isAtRisk());

            ////BasicDBObject setQuery = new BasicDBObject("$set", newFields);
            //setQuery.append("$set", newFields);



           // clientCollection.updateOne(idQuery, setQuery);
            //clientRepository.save(curatedClient);





        }


        //Optional<Client> id = clientRepository.findById(String.valueOf(curatedClient.getId()));
        //System.out.println(id);





    }


}
