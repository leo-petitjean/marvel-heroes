var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "test_3_heroes";

const insertHeroes = (db, callback) => {
    const collection = db.collection(collectionName);

    const heroes = [];
    fs.createReadStream('all-heroes.csv')
        .pipe(csv()).on('data', data => {
            heroes.push({
                "id" : data['id'],
                "name" : data['name'],
                "description" : data['description'],
                "imageURL" : data['imageURL'],
                "backgroundImageURL" : data['backgroundImageURL'],
                "externalLink" : data['externalLink'],
                "identity": {
                    "secretIdentities" : process_data(data['secretIdentities']),
                    "birthPlace": data['birthPlace'],
                    "occupation": data['occupation'],
                    "aliases": process_data(data['aliases']),
                    "alignment": data['alignment'],
                    "firstAppearance": data['firstAppearance'],
                    "yearAppearance": parseInt(data['yearAppearance']),
                    "universe": data['universe']
                },
                "appearance":{
                    "gender": data['gender'],
                    "race": data['race'],
                    "type": data['type'],
                    "height": parseInt(data['height']),
                    "weight": parseInt(data['weight']),
                    "eyeColor": data['eyeColor'],
                    "hairColor": data['hairColor']
                },
                "teams": process_data(data['teams']),
                "powers": process_data(data['powers']),
                "partners": process_data(data['partners']),
                "skills": {
                    "intelligence": parseInt(data['intelligence']),
                    "strength": parseInt(data['strength']),
                    "speed": parseInt(data['speed']),
                    "durability" : parseInt(data['durability']),
                    "power" : parseInt(data['power']),
                    "combat" : parseInt(data['combat'])
                },
                "creators" : process_data(data['creators'])
            })
        }).on('end', () =>{
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
    console.log(heroes[0]);

}

function process_data(data){
    let tampon = [];
    if (typeof(data) === "string" && data !== ''){
    let annexe = data.split(',');
    annexe.forEach(element => {
        tampon.push(element);
    });
    }
    console.log(tampon)
    return tampon;
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, result => {
        console.log(`${result.insertedCount} actors inserted`);
        client.close();
    });
});