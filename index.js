const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const filesClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
const transactionClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});

const producer = new kafka.Producer(filesClient);

const filesConsumer = new Consumer(
    filesClient,
    [
        { topic: 'files' }
    ]);

const transactionConsumer = new Consumer(
    transactionClient,
    [
        { topic: 'transactions' }
    ]);

producer.on('ready', function () {

    console.log('Producer ready, listening to file messages');
    filesConsumer.on('message', (message) => {
        console.log(`new file: ${JSON.stringify(message)}`)
        producer.send([{
            topic: 'transactions',
            messages: ['message body']
        }], () => {})
    });
    filesConsumer.on('error', (message) => {
        console.error(message);
    });
});

transactionConsumer.on('message', (message) => {
    console.log(`new transaction: ${JSON.stringify(message)}`)
});
transactionConsumer.on('error', (message) => {
    console.error(message);
});
