import Kafka from 'node-rdkafka';

function calculate_pi(digits) {
	let i = 1n;
	let x = 3n * (10n ** (BigInt(digits) + 20n));
	let pi = x;

    while (x > 0) {
        for (let j = 0; j < 100; ++j) {
            x = x * i / ((i + 1n) * 4n);
            pi += x / (i + 2n);
            i += 2n;
        }
    }

    return '3.' + (pi / (10n ** 20n)).toString(10).substr(1);
}

// O(log N)
function calculate_exponent(n, k) {
    const n_big = BigInt(value.toString().split(' ')[0])
    const k_big = BigInt(this.seen)

    const product = (k_big ** n_big).toString(10);
}

// O(2^N)
function calculate_fibonacci(n) {
    if (n <= 1) {
        return n;
    }

    return calculate_fibonacci(n - 2) + calculate_fibonacci(n - 1);
}


// O(N-2)
const fibonacci_memory = {
    0: BigInt(0),
    1: BigInt(1)
};

function calculate_fibonacci_memory(n) {
    if (n in fibonacci_memory) {
        return fibonacci_memory[n];
    }

    fibonacci_memory[n] = calculate_fibonacci_memory(n-2) + calculate_fibonacci_memory(n-1);
    return fibonacci_memory[n];
}

class Consumer {
    type;
    topic;
    kafkaBrokerList;
    seen = 0;
    
    globalConfig;

    constructor(settings, producer) {
        this.topic = settings.sourceTopic;
        this.kafkaBrokerList = settings.kafkaBrokerList;

        this.producer = producer;

        this.globalConfig = {
            'group.id': 'kafka',
            'metadata.broker.list': this.kafkaBrokerList,
        };
    }

    handleData({ value, size, topic, offset, partition, key, timestamp }) {
        console.log(`Consumed record with key ${key} and value ${value} of partition ${partition} @ offset ${offset}. Updated total count to ${++this.seen}`);
        
        // const product = calculate_pi(this.seen).toString();
        // const product = calculate_exponent(value.toString().split(' ')[0], this.seen).toString();
        // const product = calculate_fibonacci(this.seen).toString();
        const product = calculate_fibonacci_memory(this.seen).toString();

        if (this.producer) {
            this.producer.produce(product);
        }
    }

    init(type) {
        switch (type) {
            case 'CONSUMER_STANDARD_FLOWING':
            case 'CONSUMER_STANDARD_NON_FLOWING':
                this.consumer = new Kafka.KafkaConsumer(this.globalConfig, {});
                
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
                    this.consumer.consume(100);
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
        this.stream = Kafka.KafkaConsumer.createReadStream(this.globalConfig, {}, {
            topics: [this.topic]
        });
          
        this.stream
            .on('data', (message) => this.handleData(message))
            .on('error', function (err) {
                console.error('Error in our kafka consume stream');
                console.error(err);
            });
    }
}

export default { ConsumerStandardFlowing, ConsumerStandardNonFlowing, ConsumerStreaming };

