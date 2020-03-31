const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'file-parsers' });

const run = async () => {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: 'files' });

    await consumer.run({
        eachMessage: async ({ message }) => {
            console.log(`new file: ${JSON.stringify(message)}`);

            const data = require(`./data/${message.value}.json`);

            await Promise.all(data.map(record => {
                return producer.send({
                    topic: 'transactions',
                    messages: [
                        { value: JSON.stringify(record) },
                    ],
                    key: record.id,
                })
            }));
        },
    })
};

run().catch(console.error);
