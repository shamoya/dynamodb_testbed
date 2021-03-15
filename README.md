Dynamodb testbed:

This test assume a table named Test is created in AWS Dynamodb service, and that capacity units
are to a level which will allow a batch get operation of 100 items, each of 78KB to be performed
without passing the capacity unit limit.

According to https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
we expect up to 16MB, in as many as 100 items to be returned in a single batch get operation.
Here we request for: 100 * 78KB ~= 7.8MB.

If the test doesn't print Unexpected Unprocessed, all went as expected.
Otherwise it prints the amount of unproccessed items returned by the batch get opertion
It also prints the capacity units used if this was the case.
The amount of data actaully returned can be calculated with: (100 - Unprocessed items) * 78KB.

Usage:
1. Create a Test table, with primary key of "Dat"e and Sort key of "Index" with enough capacity.
2. cmake . && make
3. run put_items
4. run get_items

