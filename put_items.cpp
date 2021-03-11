#include <iostream>
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/PutItemRequest.h>
#include <aws/dynamodb/model/PutItemResult.h>

static const char* table = "Test1";
static std::string date = "20210101";
static const int num_indexes = 1000;
static const int MAX_SIZE = 26;
static const char letters[MAX_SIZE] = {
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q',
        'r', 's', 't', 'u', 'v', 'w', 'x',
        'y', 'z'
};
static const int metadata_size = 60;
static const int x_size = 77150;



std::string random_string(int n)
{
    std::string ran(n, ' ');

    for (int i = 0; i < n; i++)
        ran[i] = letters[rand() % MAX_SIZE];

    return ran;
}


int main()
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        Aws::Client::ClientConfiguration clientConfig;
        Aws::DynamoDB::DynamoDBClient dynamoClient(clientConfig);

        Aws::DynamoDB::Model::PutItemRequest pir;
        pir.SetTableName(table);

        Aws::DynamoDB::Model::AttributeValue name;
        name.SetS(date);
        pir.AddItem("X", name);

        Aws::DynamoDB::Model::AttributeValue data;
        std::string metadata = random_string(metadata_size);
        data.SetS(metadata);
        pir.AddItem("data", data);

        const Aws::DynamoDB::Model::PutItemOutcome result = dynamoClient.PutItem(pir);
        if (!result.IsSuccess()) {
            std::cout << result.GetError().GetMessage() << std::endl;
            return 1;
        }

        for (int x = 0; x < num_indexes; x++) {
            Aws::DynamoDB::Model::PutItemRequest pir;
            pir.SetTableName(table);

            Aws::DynamoDB::Model::AttributeValue name;
            std::string x_name = date + "#" + std::to_string(x);
            name.SetS(x_name);
            pir.AddItem("X", name);

            Aws::DynamoDB::Model::AttributeValue data;
            std::string x_data =  random_string(x_size);
            data.SetS(x_data);
            pir.AddItem("data", data);

            const Aws::DynamoDB::Model::PutItemOutcome result = dynamoClient.PutItem(pir);
            if (!result.IsSuccess()) {
                std::cout << result.GetError().GetMessage() << std::endl;
                return 1;
            }
        }

        std::cout << "Done!" << std::endl;
    }
    Aws::ShutdownAPI(options);
    return 0;

}
