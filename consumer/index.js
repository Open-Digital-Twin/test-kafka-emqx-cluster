const Kafka = require('node-rdkafka');


const kafkaBrokerList = process.env.BOOTSTRAP_SERVERS;
const sourceTopic = process.env.SOURCE_TOPIC;
const sinkTopic = process.env.SINK_TOPIC;

console.log({
    kafkaBrokerList, sourceTopic, sinkTopic
})

const producer = new Kafka.Producer({
    'metadata.broker.list': kafkaBrokerList
});

producer.connect();

const consumer = new Kafka.KafkaConsumer({
    'group.id': 'kafka',
    'metadata.broker.list': kafkaBrokerList,
}, {});

consumer.connect();

process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
});


let seen = 0;
consumer
    .on('ready', () => {
        console.log(`Consuming records from topic "${sourceTopic}"`);
        consumer.subscribe([sourceTopic]);
        consumer.consume();
    })
    .on('data', ({ value, size, topic, offset, partition, key, timestamp }) => {
        console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++seen}`);

        producer.produce(
            sinkTopic,
            null,
            Buffer.from(value),
            null,
            Date.now(),
        );
    });


producer.on('event.error', function(err) {
    console.error('Error from producer');
    console.error(err);
});
  
producer.setPollInterval(100);