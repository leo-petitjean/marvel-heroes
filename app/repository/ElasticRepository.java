package repository;

import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {

        String query = "{\n"+
            "\"size\": "+size+",\n"+
            "\"from\": "+size * (page - 1)+",\n"+
            "\"query\": {\n"+
                  "\"query_string\" : {\n"+
                      "\"query\" : "+input+ ",\n"+
                      "\"fields\" : ["name.input^10", "aliases.keyword^5", "secretIdentities.input^2" ]\n"+
                  "}\n"+
              "}\n"+
          "}";
        return wsClient.url(elasticConfiguration.uri + "heroes/_search")
                 .post(Json.parse(query))
                 .thenApply(response -> {
                    List<SearchedHero> heroes = parseSource(response.asJson();.get("hits").get("hits"));
                    int tot = response.asJson();.get("hits").get("total").get("value").asInt();
                    int tot_Page = (int) (tot / size) + 1;
                    PaginatedResults results = new PaginatedResults<>(tot,page,tot_Page,heroes);
                    return results;
                     
               });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {

        String query = "{\n" +
            "\"suggest\": {\n" +
            "\"suggestion\" : {\n" +
                "\"prefix\" : \""+input+"\", \n" +
                "\"completion\" : { \n" +
                "\"field\" : \"suggest\" \n" +
                        "}\n" +
                    "}\n" +
                "}\n" +
            "}";



    
        return wsClient.url(elasticConfiguration.uri + "heroes/_search")
                .post(Json.parse(query))
                 .thenApply(response -> {
                    ArrayList<SearchedHero> results_research = new ArrayList<SearchedHero>();
                    JsonNode results_point = Json.parse(response.getBody()).get("hits").get("hits");
                    for (int i = 0; i < results_point.size(); i++) {
                        ((ObjectNode) results_point.get(i).get("_source")).put("id", results_point.get(i).get("_id"));
                        SearchedHero results_hero= SearchedHero.fromJson(results_point.get(i).get("_source"));
                        results_research.add(results_hero);
                    }
                    return results_research;
                 });
    }
}
