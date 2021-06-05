var mqtt = require('mqtt')
var client = mqtt.connect('mqtt://test.mosquitto.org');
var MongoClient = require('mongodb');
var url = require('url');
let cachedDb = null;
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
    // message is Buffer
    console.log("")
    console.log("-------------")
    console.log("topc: ", topic)
    console.log("message", message.toString())


    if (topic == "esp32/pi/FAESA/ss2h35/temperature") {

        sedToMongo('temperature', message.toString())

    } else if (topic == "esp32/pi/FAESA/ss2h35/humidity") {

        sedToMongo('humidity', message.toString())

    } else if (topic == "esp32/pi/FAESA/ss2h35/hic") {

        sedToMongo('hic', message.toString())

    } else {
        console.log("default topic: ", topic)
        console.log("default message: ", message)
    }

    console.log("-------------")
    console.log("")

})