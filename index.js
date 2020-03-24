const kafka = require('kafka-node');
const Consumer = kafka.Consumer;
const filesClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});
const transactionClient = new kafka.KafkaClient({kafkaHost: 'localhost:9092'});

const producer = new kafka.Producer(filesClient, {
    partitionerType: 3,
});

const filesConsumer = new Consumer(
    filesClient,
    [
        { topic: 'files' }
    ]);

const transactionConsumer = new kafka.ConsumerGroup(
    {
        kafkaHost: 'localhost:9092',
        groupId: 'transaction-consumer-group'
    },
    ['transactions', 'RebalanceTopic', 'RebalanceTest']);

producer.on('ready', function () {

    console.log('Producer ready, listening to file messages');
    filesConsumer.on('message', (message) => {
        console.log(`new file: ${JSON.stringify(message)}`)

        const data = require(`./data/${message.value}.json`);

        producer.send(data.map(record => {
            const key = record.id;

            return {
                topic: 'transactions',
                messages: new kafka.KeyedMessage(key, JSON.stringify(record)),
                key
            }
        }), () => {})
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
