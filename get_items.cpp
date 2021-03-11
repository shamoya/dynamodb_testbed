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


static const char* table = "Test1";
static std::string date = "20210101";
static const int num_indexes = 1000;
static const int DYNAMODB_MAX_ITEMS_BATCH_GET = 100;
static const int first_delay_ms = 50;
static const int last_delay_ms = 100;
static const int max_delay_ms = 2000;


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
        req.AddKey("X", name);
        const Aws::DynamoDB::Model::GetItemOutcome& result = dynamoClient.GetItem(req);

        if (!result.IsSuccess()) {
            std::cout << result.GetError().GetMessage() << std::endl;
            return 1;
        }

        int current_delay = 0;
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
                Aws::DynamoDB::Model::AttributeValue xKey;
                xKey.SetS(date + "#" + std::to_string(x_index));
                xKeys.emplace(Aws::String("X"), xKey);
                keyAttrs.AddKeys(xKeys);
            }

            req.AddRequestItems(tableName, keyAttrs);
            const Aws::DynamoDB::Model::BatchGetItemOutcome& result = dynamoClient.BatchGetItem(req);
            if (result.IsSuccess()) {
                for(const auto& var : result.GetResult().GetResponses()) {
                    for (const auto& item : var.second) {
                        const Aws::Vector<Aws::String>& flds = Aws::Utils::StringUtils::Split(item.at("X").GetS(),'#');
                        xitems[std::stoi(flds[1])] = item.at("data").GetS();
                    }
                }

                const Aws::Map<Aws::String, Aws::DynamoDB::Model::KeysAndAttributes> unprocessed = result.GetResult().GetUnprocessedKeys();
                if (!unprocessed.empty()) {
                    for (const auto& var: unprocessed) {
                        for (const auto& item : var.second.GetKeys()) {
                            const Aws::Vector<Aws::String> &flds = Aws::Utils::StringUtils::Split(item.at("X").GetS(), '#');
                            x_list.push_back(std::stoi(flds[1]));
                        }
                    }

                    if (!current_delay) {
                        std::vector<int> delays_ms(first_delay_ms - last_delay_ms + 1);
                        std::iota(delays_ms.begin(), delays_ms.end(), first_delay_ms);
                        std::uniform_int_distribution<> dis(0, std::distance(delays_ms.begin(), delays_ms.end()) - 1);
                        std::random_device rd;
                        std::mt19937 prng(rd());
                        auto delay_iter = delays_ms.begin();
                        std::advance(delay_iter, dis(prng));
                        current_delay = *delay_iter;
                    } else {
                        current_delay = std::min(current_delay *2, max_delay_ms);
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(current_delay));
                } else if (current_delay != 0) {
                    current_delay = 0;
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