var mqtt = require('mqtt')
var client = mqtt.connect('mqtt://test.mosquitto.org');
var MongoClient = require('mongodb');
var url = require('url');
let cachedDb = null;

var express = require('express');
var app = express();

app.get('/', function(req, res) {
    res.send('ok');
});

app.listen(process.env.PORT || 5000)

require('dotenv').config()


async function connectToDatabase(uri) {
    debugger
    if (cachedDb)
        return cachedDb;

    const client = await MongoClient.connect(uri, {
        //disabled warnings
        useNewUrlParser: true,
        useUnifiedTopology: true
    });

    const dbName = url.parse(uri).pathname.substr(1);

    const db = client.db(dbName);

    cachedDb = db;

    return db;
}

async function sedToMongo(topic, value) {

    const db = await connectToDatabase(process.env.CONNECTIONSTRING);

    const collection = db.collection(topic);

    await collection.insertOne({
        topic,
        value,
        subscribeAt: new Date(),
    })

}

client.on('connect', function() {

    client.subscribe('esp32/pi/FAESA/ss2h35/temperature', function(err) {

    })
    client.subscribe('esp32/pi/FAESA/ss2h35/humidity', function(err) {

    })
    client.subscribe('esp32/pi/FAESA/ss2h35/hic', function(err) {

    })
})

client.on('message', function(topic, message) {

    if (topic == `${process.env.CONNECTIONMQTT}/temperature`) {

        sedToMongo('temperature', message.toString())

    } else if (topic == `${process.env.CONNECTIONMQTT}/humidity`) {

        sedToMongo('humidity', message.toString())

    } else if (topic == `${process.env.CONNECTIONMQTT}/hic`) {

        sedToMongo('hic', message.toString())

    } else {
        sedToMongo('error', message.toString())
    }

})