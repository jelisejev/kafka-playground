const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();

const json = {
    "data": "123"
};


const run = async () => {
    await producer.connect();

    const data = [];
    for (var i = 0; i < 50000; i++) {
        data.push(json);
    }

    const start = new Date();
    console.log(start);
    await Promise.all(data.map(record => {
        return producer.send({
            // compression: CompressionTypes.GZIP,
            topic: 'loadtest',
            messages: [
                { value: JSON.stringify(record) },
            ],
            key: record.id,
        })
    }));
    const end = new Date();
    console.log(end);
    console.log((end.getTime() - start.getTime()) / 1000)
};

run().catch(console.error).then(() => process.exit(1));
