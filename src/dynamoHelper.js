const timeout = ms => new Promise(res => setTimeout(res, ms))
import AWS from 'aws-sdk'

export default ({ awsOptions, tableName, indexName }) => {
    const docClient = new AWS.DynamoDB.DocumentClient(awsOptions)
    
    async function scan({ limit, startKey, options = {} }) {
        let results = []
        let hasMore = true

        while (hasMore && results.length < limit || !limit) {
            let { Items = [], LastEvaluatedKey } = await docClient.scan({
                TableName: tableName,
                IndexName: indexName,
                Limit: limit,
                ExclusiveStartKey: startKey || undefined,
                ...options,
            }).promise()

            results = results.concat(Items)
            startKey = LastEvaluatedKey
            hasMore = !!LastEvaluatedKey
        }

        return {
            results,
            hasMore,
        }
    }

    async function query({ keyName, keyValue, limit, startKey, options = {} }) {
        let results = []
        let hasMore = true
        while (hasMore && results.length < limit || !limit) {
            let { Items = [], LastEvaluatedKey } = await docClient.query({
                TableName: tableName,
                IndexName: indexName,
                KeyConditionExpression: '#hkey = :hkey',
                ExpressionAttributeValues: {
                    ':hkey': keyValue,
                },
                ExpressionAttributeNames: {
                    '#hkey': keyName,
                },
                Limit: limit,
                ExclusiveStartKey: startKey || undefined,
                ...options,
            }).promise()

            results = results.concat(Items)
            startKey = LastEvaluatedKey
            hasMore = !!LastEvaluatedKey
        }

        return {
            results,
            hasMore,
        }        
    }

    async function batchGet({ keys, options = {} }) {
        let results = []
        while (keys.length > 0) {
            let Keys = keys.splice(0, 100)
            let { Responses, UnprocessedKeys } = await docClient.batchGet({
                RequestItems: {
                    [tableName]: { Keys }
                },
                ...options,
            }).promise()

            results = results.concat(Responses)

            let remaining = UnprocessedKeys.Keys
            if (remaining.length > 0) {
                await timeout(Math.random() * remaining.length)
                keys = remaining.concat(keys)
            }
        }

        return results
    }

    async function batchDelete({ keys }) {
        while (keys.length > 0) {
            let Keys = keys.splice(0, 25)
            await docClient.batchWrite({
                RequestItems: {
                    [tableName]: Keys.map( Key => ({
                        DeleteRequest: { Key }
                    }))
                },
            }).promise()
        }
    }
    const getOne = ({ key, options = {} }) => {
        return docClient.get({
            TableName: tableName,
            Key: key,
            ...options,
        }).promise()
            .then(({ Item }) => Item)
    }

    const deleteItem = ({ key, options = {} }) => {
        return docClient.delete({
            TableName: tableName,
            Key: key,
            ...options,
        }).promise()
    }

    const putItem = ({ attributes, options = {} }) => {
        return docClient.put({
            TableName: tableName,
            Item: attributes,
            ...options,
        }).promise()
            .then(() => attributes)
    }

    return {
        scan,
        query,
        batchGet,
        getOne,
        putItem,
        deleteItem,
        batchDelete,
    }
}