package net.ditlef.kafka_java_example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Main {
    static Properties kafkaPropsProducer = new Properties();
    static Properties kafkaPropsConsumer = new Properties();

    public static void main(String[] args) {
        menu();

        readProperties();
        produce();
        consume();
    } // main

    /*- Read Properties ------------------------------------------------------ */
    private static void readProperties() {

        try {
            kafkaPropsProducer.load(new FileReader("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            kafkaPropsConsumer.load(new FileReader("consumer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*- Menu ---------------------------------------------------------------- */
    public static void menu(){
        int runProgram = 1;
        while(runProgram > 0) {
            // Headline
            System.out.println("                                                    ");
            System.out.println("  _  __      __ _                _                  ");
            System.out.println(" | |/ /     / _| |              | |                 ");
            System.out.println(" | ' / __ _| |_| | ____ _       | | __ ___   ____ _ ");
            System.out.println(" |  < / _` |  _| |/ / _` |  _   | |/ _` \\ \\ / / _` |");
            System.out.println(" | . \\ (_| | | |   < (_| | | |__| | (_| |\\ V / (_| |");
            System.out.println(" |_|\\_\\__,_|_| |_|\\_\\__,_|  \\____/ \\__,_| \\_/ \\__,_|");
            System.out.println("                                                    ");

            // Scanner
            Scanner scanner = new Scanner(System.in);


            // Print menu
            System.out.println("--- MENU --- ");
            System.out.println("[1] Produce event");
            System.out.println("[2] Consume topic");
            System.out.println("[-1] End program");
            System.out.println("");

            // Ask for menu
            System.out.print("Menu choice: ");
            int menu = scanner.nextInt();

            // Go trough menu possibilities
            if(menu == 1){
                System.out.println("--- PRODUCE EVENT ---");
                produce();
            }
            else if(menu == 2){
                System.out.println("--- CONSUME TOPIC ---");
                consume();
            }
            else{
                System.out.println("--- END PROGRAM ---");
                runProgram = 0;
            }

        } // while runProgram
    } // menu



    /*- Consume -------------------------------------------------------------- */
    private static void consume() {
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String>(kafkaPropsConsumer); // create consumer
        consumer.subscribe(Collections.singleton("quickstart-events"));

        // Pull batch of messages
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("topic = %s, partition = %s, offset = %d, customer = %s, country = %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /*- Produce -------------------------------------------------------------- */
    private static void produce() {
        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(kafkaPropsProducer); // create the producer itself
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("quickstart-events","my-key", "test"); // ProducerRecord object

        // Send message
        try{
            producer.send(record);
            producer.flush();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
