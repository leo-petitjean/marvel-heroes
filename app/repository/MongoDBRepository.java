package repository;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import utils.HeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        return HeroSamples.staticHero(heroId);
        // TODO
        String query = "{ id : \"" + heroId + "\"}";
        // Document document = Document.parse(query);
        // return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
        //         .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        List<Document> pipeline = new ArrayList<>();
        List<String> annexe_string = new ArrayList<String>();
        annexe_string.add("{ $match: {\"identity.yearAppearance\" : {\"$ne\": \"\"}}}");
        annexe_string.add("{ $group: { _id: { yearAppearance :\"$identity.yearAppearance\",universe :\"$identity.universe\"}, count: {$sum: 1}}}");
        annexe_string.add("{$group: { _id: \"$_id\", byUniverse: {$push: {universe: \"$_id.universe\", count: \"$count\"}}}})");
        annexe_string.add("{ $limit: " + top + "}");

        for (i:annexe_string){
            pipeline.add(Document.parse(i));
        }
        // TODO
        
        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                .thenApply(documents -> {
                   return documents.stream()
                                   .map(Document::toJson)
                                   .map(Json::parse)
                                  .map(jsonNode -> {
                                       int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);
        
                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {

         List<Document> pipeline = new ArrayList<>();
         List<String> annexe_string = new ArrayList<String>();
        annexe_string.add("{ $unwind: \"$powers\"}");
        annexe_string.add("{ $group: { _id: \"$powers\", count: {$sum: 1}}}");
        annexe_string.add("{ $sort:  {count: -1}}");
        annexe_string.add("{$sort: {\"_id.yearAppearance\": -1}}");

        for (i:annexe_string){
            pipeline.add(Document.parse(i));
        }
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
         List<Document> pipeline = new ArrayList<>();
         List<String> annexe_string = new ArrayList<String>();
        annexe_string.add("{ $group: { _id: \"$identity.universe\", count: {$sum: 1}}}");

        for (i:annexe_string){
            pipeline.add(Document.parse(i));
        }
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

}
