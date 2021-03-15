#include <iostream>
#include <vector>
#include <chrono>
#include <numeric>
#include <random>
#include <aws/core/Aws.h>
#include <aws/core/utils/Outcome.h>
#include <aws/dynamodb/DynamoDBClient.h>
#include <aws/dynamodb/model/AttributeDefinition.h>
#include <aws/dynamodb/model/GetItemRequest.h>
#include <aws/dynamodb/model/BatchGetItemRequest.h>


static const char* table = "Test";
static std::string date = "20210101";
static const int num_indexes = 1000;
static const int DYNAMODB_MAX_ITEMS_BATCH_GET = 100;


int main()
{
    Aws::SDKOptions options;
    Aws::InitAPI(options);
    {
        std::map<int, std::string> xitems;
        const Aws::String tableName(table);
        Aws::Client::ClientConfiguration clientConfig;
        Aws::DynamoDB::DynamoDBClient dynamoClient(clientConfig);

        auto start = std::chrono::high_resolution_clock::now();
        Aws::DynamoDB::Model::GetItemRequest req;
        req.SetTableName(tableName);

        Aws::DynamoDB::Model::AttributeValue name;
        name.SetS(date);
        req.AddKey("Day", name);

        Aws::DynamoDB::Model::AttributeValue index;
        index.SetS("-1");
        req.AddKey("Index", index);

        const Aws::DynamoDB::Model::GetItemOutcome& result = dynamoClient.GetItem(req);

        if (!result.IsSuccess()) {
            std::cout << result.GetError().GetMessage() << std::endl;
            return 1;
        }

        int sub_x_list_counter = 0;
        std::vector<int> x_list(num_indexes);
        std::iota(std::begin(x_list), std::end(x_list), 0);

        do {
            std::vector<int> sub_x_list;
            Aws::DynamoDB::Model::BatchGetItemRequest req;
            Aws::DynamoDB::Model::KeysAndAttributes keyAttrs;

            if (x_list.size() - sub_x_list_counter > DYNAMODB_MAX_ITEMS_BATCH_GET)
                sub_x_list = {x_list.begin() + sub_x_list_counter,
                              x_list.begin() + sub_x_list_counter + DYNAMODB_MAX_ITEMS_BATCH_GET};
            else
                sub_x_list = {x_list.begin() + sub_x_list_counter, x_list.end()};

            for (const auto x_index : sub_x_list) {
                Aws::Map<Aws::String, Aws::DynamoDB::Model::AttributeValue> xKeys;
                Aws::DynamoDB::Model::AttributeValue name;
                name.SetS(date);
                xKeys.emplace(Aws::String("Day"), name);
                Aws::DynamoDB::Model::AttributeValue index;
                index.SetS(std::to_string(x_index));
                xKeys.emplace(Aws::String("Index"), index);
                keyAttrs.AddKeys(xKeys);
            }

            req.AddRequestItems(tableName, keyAttrs);
            req.SetReturnConsumedCapacity(Aws::DynamoDB::Model::ReturnConsumedCapacity::INDEXES);
            const Aws::DynamoDB::Model::BatchGetItemOutcome& result = dynamoClient.BatchGetItem(req);
            if (result.IsSuccess()) {
                for(const auto& var : result.GetResult().GetResponses()) {
                    for (const auto& item : var.second)
                        xitems[std::stoi(item.at("Index").GetS())] = item.at("Data").GetS();
                }

                const Aws::Map<Aws::String, Aws::DynamoDB::Model::KeysAndAttributes> unprocessed = result.GetResult().GetUnprocessedKeys();
                if (!unprocessed.empty()) {
                    /*
                     *  According to https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
                     *  we expect up to 16MB, in as many as 100 items to be returned in a single batch get operation.
                     *  Here we request for: DYNAMODB_MAX_ITEMS_BATCH_GET * x_size ~= 7.8MB
                     *  In reality we get around 3.5MB of data in a single operation.
                     *  The amount of data returned can be calculated with: (100 - Unprocessed items) * 78KB.
                     */
                    std::cout << "Unexpected Unprocessed " << unprocessed.at(tableName).GetKeys().size() << std::endl;
                    for (const auto& var: unprocessed) {
                        for (const auto& item : var.second.GetKeys())
                            x_list.push_back(std::stoi(item.at("Index").GetS()));
                    }
                }
                sub_x_list_counter += sub_x_list.size();
            }
        } while (x_list.size() - sub_x_list_counter > 0);

        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Done! " <<  std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() <<std::endl;
    }
    Aws::ShutdownAPI(options);
    return 0;

}
