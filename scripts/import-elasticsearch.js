const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const heroesIndexName = 'heroes'

async function run () {
    client.indices.create({ index: 'test_dans_ma_rue', body: { 
            mappings: {
                properties: {
                    "name":{
                        "type" : "completion"
                    },
                    "secretIdentities":{
                        "type": "completion"
                    }
                }
            } 
        }
    }, (err, resp) => {
        if (err) console.trace(err.message);
      });
      const bulk_Size = 5000;
      let bulk_heroes = [];
      fs.createReadStream('all-heroes.csv')
        .pipe(csv({
            separator: ','
        }))
        .on('data', (data) => {
            console.log(data)
            let tampon=[]
            if (data['secretIdentities'] !== ''  ){
            let annexe = data['secretIdentities'].split(',');
            annexe.forEach(element => {
                tampon.push({
                    "input": element,
                    "weight": 3
                })
            });}
            if (bulk_heroes.length <= bulk_Size){
            bulk_heroes.push({
                "id" : data['id'],
                "name" : {
                    "input" : data['name'],
                    "weight" : 10},
                "description" : data['description'],
                "imageURL" : data['imageURL'],
                "backgroundImageURL" : data['backgroundImageURL'],
                "externalLink" : data['externalLink'],
                "secretIdentities" : tampon,
                "birthPlace": data['birthPlace'],
                "occupation": data['occupation'],
                "aliases": data['aliases'],
                "alignment": data['alignment'],
                "firstAppearance": data['firstAppearance'],
                "yearAppearance": data['yearAppearance'],
                "universe": data['universe'],
                "gender": data['gender'],
                "race": data['race'],
                "type": data['type'],
                "height": data['height'],
                "weight": data['weight'],
                "eyeColor": data['eyeColor'],
                "hairColor": data['hairColor'],
                "teams": data['teams'],
                "powers": data['powers'],
                "partners": data['partners'],
                "intelligence": data['intelligence'],
                "strength": data['strength'],
                "speed": data['speed'],
                "durability" : data['durability'],
                "power" : data['power'],
                "combat" : data['combat'],
                "creators" : data['creator']
            })
            tampon = []}
        if (bulk_heroes.length >= bulk_Size){
            client.bulk(createBulkInsertQuery(bulk_heroes), (err, resp) => {
                if (err) console.trace(err.message);
                else console.log(`Inserted ${resp.body.items.length} heroes`);
                client.close();
              });
            bulk_heroes =[]
            bulk_heroes.push({
                "id" : data['id'],
                "name" : {
                    "input" : data['name'],
                    "weight" : 10},
                "description" : data['description'],
                "imageURL" : data['imageURL'],
                "backgroundImageURL" : data['backgroundImageURL'],
                "externalLink" : data['externalLink'],
                "secretIdentities" : tampon,
                "birthPlace": data['birthPlace'],
                "occupation": data['occupation'],
                "aliases": data['aliases'],
                "alignment": data['alignment'],
                "firstAppearance": data['firstAppearance'],
                "yearAppearance": data['yearAppearance'],
                "universe": data['universe'],
                "gender": data['gender'],
                "race": data['race'],
                "type": data['type'],
                "height": data['height'],
                "weight": data['weight'],
                "eyeColor": data['eyeColor'],
                "hairColor": data['hairColor'],
                "teams": data['teams'],
                "powers": data['powers'],
                "partners": data['partners'],
                "intelligence": data['intelligence'],
                "strength": data['strength'],
                "speed": data['speed'],
                "durability" : data['durability'],
                "power" : data['power'],
                "combat" : data['combat'],
                "creators" : data['creator']
            })
        }
    }).on('end', () => {
        client.bulk(createBulkInsertQuery(bulk_heroes), (err, resp) => {
            if (err) console.trace(err.message);
            else console.log(`Inserted ${resp.body.items.length} heroes`);
            client.close();
          });
        console.log('Terminated!');
    });
}
function createBulkInsertQuery(heroes) {
    const body = heroes.reduce((acc, heroes) => {
        //const {timestamp, object_id, annee_declaration, mois_declaration, type, sous_type, code_postal, ville, arrondissement,
        //prefixe, intervenant, conseil_de_quartier, location} = annomalie;
        acc.push({ index: { _index: heroesIndexName, _type: '_doc', _id: heroes.id } })
        acc.push(heroes)
        return acc
    }, []);
    
    return { body };
    }
run().catch(console.error);

console.log("TODO ;-)");
