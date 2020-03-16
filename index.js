const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});

const consumer = new Consumer(
    client,
    [
        { topic: 'files' }
    ]);

console.log('Listening to messages');
consumer.on('message', (message) => {
    console.log(message);
});
consumer.on('error', (message) => {
    console.error(message);
});
