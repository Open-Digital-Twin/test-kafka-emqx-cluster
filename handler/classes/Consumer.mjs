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

    handleData({ value, size, topic, offset, partition, key, timestamp }) {
        console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++this.seen}`);
        
        // TODO: Compute something with value then assign to product.
        const product = value;

        if (this.producer) {
            this.producer.produce(product);
        }
    }

    init(type) {
        switch (type) {
            case 'CONSUMER_STANDARD_FLOWING':
            case 'CONSUMER_STANDARD_NON_FLOWING':
                this.consumer = new Kafka.KafkaConsumer({
                    'group.id': 'kafka',
                    'metadata.broker.list': this.kafkaBrokerList,
                }, {});
                
                process.on('SIGINT', () => {
                    console.log('\nDisconnecting consumer ...');
                    this.consumer.disconnect();
                });
            case 'CONSUMER_STREAMING':
                break;
            default:
                console.log('Wrong consumer type.');
        }
    }
}

export class ConsumerStandardFlowing extends Consumer {
    type = 'CONSUMER_STANDARD_FLOWING';
    consumer;

    constructor(settings, producer) {
        super(settings, producer);
        
        this.init(this.type);
    }
    
    consume() {
        this.consumer.connect();

        this.consumer
            .on('ready', () => {
                console.log(`Consuming records from topic "${this.topic}"`);
                this.consumer.subscribe([this.topic]);
                this.consumer.consume();
            })
            .on('data', (message) => this.handleData(message));
    }
}

export class ConsumerStandardNonFlowing extends Consumer {
    type = 'CONSUMER_STANDARD_NON_FLOWING';
    consumer;

    constructor(settings, producer) {
        super(settings, producer);
        
        this.init(this.type);
    }
    
    consume() {
        this.consumer.connect();

        this.consumer
            .on('ready', () => {
                console.log(`Consuming records from topic "${this.topic}"`);
                this.consumer.subscribe([this.topic]);

                setInterval(() => {
                    this.consumer.consume(1);
                }, 100);
            })
            .on('data', (message) => this.handleData(message));
    }
}

export class ConsumerStreaming extends Consumer {
    type = 'CONSUMER_STREAMING';
    stream;
    
    constructor(settings, producer) {
        super(settings, producer);
        
        this.init(this.type);
    }

    consume() {
        this.stream = Kafka.KafkaConsumer.createReadStream({}, {}, {
            topics: [this.topic]
        });
          
        this.stream
            .on('data', (message) => this.handleData(message));
    }
}

export default { ConsumerStandardFlowing, ConsumerStandardNonFlowing, ConsumerStreaming };

