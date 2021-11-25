const settings = {
    kafkaBrokerList: process.env.BOOTSTRAP_SERVERS || 'undefined',
    sourceTopic: process.env.SOURCE_TOPIC || 'undefined',
    sinkTopic: process.env.SINK_TOPIC || 'undefined'
}

console.log(settings);

// RUN MODES:
//
// Streaming Mode.
//
// Standard Mode:
//  Flowing mode. This mode flows all of the messages it can read by maintaining an infinite loop in the event loop. It only stops when it detects the consumer has issued the unsubscribe or disconnect method.
// //  Non-flowing mode. This mode reads a single message from Kafka at a time manually.

import { ConsumerStandardFlowing, ConsumerStandardNonFlowing, ConsumerStreaming } from './classes/Consumer';
import { ProducerStandard, ProducerStreaming } from './classes/Producer';

let producer;
switch (process.env.RUN_MODE_PRODUCER) {
    case 0:
    case 'PRODUCER_STANDARD':
        producer = new ProducerStandard(settings);        
        break;
    case 1:
    case 'PRODUCER_STREAMING':
        producer = new ProducerStreaming(settings);
        break;
    default:
        producer = new ProducerStandard(settings);
        break;
}

let consumer;
switch (process.env.RUN_MODE_CONSUMER) {
    case 0:
    case 'CONSUMER_STANDARD_FLOWING':
        consumer = new ConsumerStandardFlowing(settings, producer);        
        break;
    case 1:
    case 'CONSUMER_STANDARD_NON_FLOWING':
        consumer = new ConsumerStandardNonFlowing(settings, producer);
        break;
    case 2:
    case 'CONSUMER_STREAMING':
        consumer = new ConsumerStreaming(settings, producer);
        break;
    default:
        consumer = new ConsumerStandardFlowing(settings, producer);        
        break;
}

console.log({consumer, producer})

consumer.consume();
