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
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`read from topic ${topic}, partition ${partition}`);
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

            const offset = {
                topic,
                partition,
                offset: String(Number(message.offset) + 1),
            };
            console.log(`comitting offset: ${JSON.stringify(offset)}`);
            await consumer.commitOffsets([offset])
            console.log(`done`);
        },
    })
};

run().catch(console.error);
