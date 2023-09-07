import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class Example {
    Example() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        String TOPIC_NAME = "customers";
        Collection<String> TOPIC_LIST = Arrays.asList(TOPIC_NAME);
        int NUM_PARTITIONS = 1;
        short RF = 1;

        // List topics

        try {
            ListTopicsResult topics = admin.listTopics();
            topics.names().get().forEach(System.out::println);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Create a topic

        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);

        try {
            TopicDescription topicDescription = demoTopic.topicNameValues().get("customers").get();
            System.out.println("Description of customers topic" + topicDescription);

            if (topicDescription.partitions().size() != NUM_PARTITIONS) {
                System.out.println("Topic has wrong number of partitions");
                System.exit(-1);
            }
        } catch (ExecutionException e) {
            if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
                e.printStackTrace();
                throw e;
            }

            System.out.println("Topic " + TOPIC_NAME + " does not exist.");

            CreateTopicsResult newTopic = admin.createTopics(Collections.singleton(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, RF)));

            if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
                System.out.println("Topic has wrong number of partitions.");
                System.exit(-1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Delete a topic

        admin.deleteTopics(TOPIC_LIST).all().get();

        try {
            TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();
            System.out.println("Topic " + TOPIC_NAME + "is still there.");
        } catch (ExecutionException e) {
            System.out.println("Topic " + TOPIC_NAME + " was deleted.");
        }

        // Close AdminClient
        admin.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new Example();
    }
}
