import Kafka from 'node-rdkafka';

class Consumer {
    type;
    topic;
    kafkaBrokerList;
    seen = 0;

    constructor(settings, producer) {
        this.topic = settings.sourceTopic;
        this.kafkaBrokerList = settings.kafkaBrokerList;

        this.producer = producer;
    }

    consume() {}

    handleData({ value, size, topic, offset, partition, key, timestamp }) {
        console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++this.seen}`);
            
        if (this.producer) {
            this.producer.produce(value);
        }
    }
}

export class ConsumerStandardFlowing extends Consumer {
    type = 'CONSUMER_STANDARD_FLOWING';
    consumer;
    
    consume() {
        this.consumer = new Kafka.KafkaConsumer({
            'group.id': 'kafka',
            'metadata.broker.list': this.kafkaBrokerList,
        }, {});

        this.consumer.connect();

        process.on('SIGINT', () => {
            console.log('\nDisconnecting consumer ...');
            this.consumer.disconnect();
        });

        this.consumer
            .on('ready', () => {
                console.log(`Consuming records from topic "${this.topic}"`);
                this.consumer.subscribe([this.topic]);
                this.consumer.consume();
            })
            .on('data', this.handleData);
    }
}

export class ConsumerStandardNonFlowing extends Consumer {
    type = 'CONSUMER_STANDARD_NON_FLOWING';
    consumer;
    
    consume(producer) {
        this.producer = producer;

        this.consumer = new Kafka.KafkaConsumer({
            'group.id': 'kafka',
            'metadata.broker.list': this.kafkaBrokerList,
        }, {});

        this.consumer.connect();

        process.on('SIGINT', () => {
            console.log('\nDisconnecting consumer ...');
            this.consumer.disconnect();
        });

        this.consumer
            .on('ready', () => {
                console.log(`Consuming records from topic "${this.topic}"`);
                this.consumer.subscribe([this.topic]);

                setInterval(() => {
                    this.consumer.consume(1);
                }, 100);
            })
            .on('data', this.handleData);
    }
}

export class ConsumerStreaming extends Consumer {
    type = 'CONSUMER_STREAMING';
    stream;
    
    consume() {
        this.stream = Kafka.KafkaConsumer.createReadStream({}, {}, {
            topics: [this.topic]
        });
          
        this.stream.on('data', this.handleData);
    }
}

export default { ConsumerStandardFlowing, ConsumerStandardNonFlowing, ConsumerStreaming };

