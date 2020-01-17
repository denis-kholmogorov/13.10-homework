import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Updates.*;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import org.bson.Document;
import org.bson.conversions.Bson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Test {

    public static void main(String[] args)
    {
        MongoClient mongoClient = new MongoClient("127.0.0.1", 27017);
        MongoDatabase database = mongoClient.getDatabase("shopping");
        MongoCollection<Document> collectionShop = database.getCollection("shops");
        MongoCollection<Document> collectionProduct = database.getCollection("product");

        int cost = 100;
        Scanner scanner = new Scanner(System.in);
        boolean accept = false;

        while(true)
        {
            String commandString = scanner.nextLine();
            String[] commands = commandString.split(" ");

            if(commands[0].equals("add_shop") && commands.length == 2)
            {
                accept = true;
                collectionShop.insertOne(new Document()
                                            .append("shopName",commands[1])
                                            .append("products", new ArrayList<String>()));
                System.out.println("Магазин " + commands[1] + "добавлени");
            }

            if(commands[0].equals("add_product") && commands.length == 3)
            {
                accept=true;
                collectionProduct.insertOne(new Document()
                                            .append("productName", commands[1])
                                            .append("cost", Integer.parseInt(commands[2])));
                System.out.println("Продукт " + commands[1] + " добавлен");
            }

            if(commands[0].equals("put_product") && commands.length == 3)
            {
                accept=true;
                Bson filter = eq("shopName", commands[2]);
                collectionShop.updateOne(filter,  addToSet("products",commands[1]));
                System.out.println("Добавить продукт " + commands[1] + " в магазин " + commands[2]);

            }

            if(commands[0].equals("put_product") && commands.length == 3)
            {
                accept=true;
                Bson filter = eq("shopName", commands[2]);
                collectionShop.updateOne(filter,  addToSet("products",commands[1]));
                System.out.println("Продукт " + commands[1] + " добавлен в магазин " + commands[2]);
            }

            if(commands[0].equals("statistics") && commands.length == 1)
            {
                accept=true;
                System.out.println("Статистика магазинов");
                AggregateIterable<Document> answer = collectionShop.aggregate(Arrays.asList(lookup("product", "products", "productName","list_product"),
                                                       unwind("$list_product"),
                                                        group("$shopName",
                                                                sum("count", 1),
                                                                avg("middleCost", "$list_product.cost"),
                                                                Accumulators.min("minCost", "$list_product.cost" ),
                                                                Accumulators.max("maxCost", "$list_product.cost" ))));

                AggregateIterable<Document> answerLess100 = collectionShop.aggregate(Arrays.asList(lookup("product", "products", "productName","list_product"),
                        unwind("$list_product"),
                        match(lt("list_product.cost", cost)),
                        group("$shopName",
                                sum("count", 1)
                               )));

                //Статистика магазинов
                answer.forEach((Block<? super Document>) doc ->{
                    System.out.println("В магазине " + doc.get("_id") + " кол-во продуктов " + doc.get("count") +
                            ", средняя цена " + doc.get("middleCost") +
                            ", минимальная цена " + doc.get("minCost") +
                            ", максимальная цена " + doc.get("maxCost"));
                });

                System.out.println();

                //Статистика магазинов с условием меньшим cost
                answerLess100.forEach((Block<? super Document>) doc ->{
                    System.out.println("В магазине " + doc.get("_id") + " кол-во продуктов меньше " + cost + " руб " + doc.get("count"));
                });

            }

            if (!accept)
            {
                System.out.println("Неверная комманда");
            }

            if(commands[0].equals("quit"))
            {
                break;
            }
        }
    }
}
/*
* add_shop shop
* add_product product 22
* put_product product shop
* */