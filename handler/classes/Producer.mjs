import Kafka from 'node-rdkafka';

class Producer {
    type = 'PRODUCER';
    topic;

    constructor(settings) {
        this.topic = settings.sinkTopic;
    }
}

export class ProducerStandard extends Producer {
    type = 'PRODUCER_STANDARD';
    producer;

    constructor(settings) {
        super(settings);

        this.producer = new Kafka.Producer({
            'metadata.broker.list': settings.kafkaBrokerList
        });

        this.producer.connect();

        this.producer.on('event.error', function(err) {
            console.error('Error from producer');
            console.error(err);
        });
          
        this.producer.setPollInterval(100);
    }

    produce(value) {
        try {
            console.log('Producing via standard mode.');

            this.producer.produce(
                this.topic,
                null,
                Buffer.from(value),
                null,
                Date.now(),
            );    
        } catch (err) {
            console.error('A problem occurred when producing');
            console.error(err);
        }
    }
}

export class ProducerStreaming extends Producer {
    type = 'PRODUCER_STREAMING';
    stream;

    constructor(settings) {
        super(settings)

        stream = Kafka.Producer.createWriteStream({
            'metadata.broker.list': settings.kafkaBrokerList
        }, {}, {
            topic: this.topic
        });
    }

    produce(value) {
        console.log('Producing via streaming mode.');

        // Writes a message to the stream
        const queuedSuccess = this.stream.write(Buffer.from(value));

        if (queuedSuccess) {
            console.log('Queued message.');
        } else {
            // Note that this only tells us if the stream's queue is full,
            // it does NOT tell us if the message got to Kafka!  See below...
            console.log('Too many messages in queue.');
        }

        this.stream.on('error', function (err) {
            console.error('Error in our kafka stream');
            console.error(err);
        });
    }
}

export default { ProducerStandard, ProducerStreaming };