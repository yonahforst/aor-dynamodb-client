import dynamoHelper from './dynamoHelper'

const noop = p => p
const buildObjParser = ([ hash, range ]) => obj => {
  if (!range) return {
    id: obj[hash],
    ...obj
  }

  return {
    id: [ obj[hash], obj[range] ].join('@'),
    ...obj,
  }
}
const removeIdFromObj = ({ id, ...obj }) => obj

const buildKeyParser = ([ hash, range ]) => id => {
  if (!range) return { 
    [ hash ]: id 
  }

  const splitId = id.split('@')
  return {
    [ hash ]: splitId[0],
    [ range ]: parseInt(splitId[1]) || 0,
  }
}

export default ({
  tableName,
  keyNames,
  parseObj = keyNames ? buildObjParser(keyNames) : noop,
  formatObj = keyNames ? removeIdFromObj : noop,
  getKey = keyNames ? buildKeyParser(keyNames) : id => ({ id }),
  awsOptions = {},
}) => {

  const dynamodb = dynamoHelper({
    tableName,
    awsOptions
  })

  const cache = []

  return {
    getOne: (_, params) => {
      return dynamodb.getOne({ 
        key: getKey(params.id)
      })
      .then(data => ({ 
        data: parseObj(data),
      }))
    },
    getMany: (_, params) => {
      return dynamodb.batchGet({ 
        keys: params.ids.map(getKey),
      })
      .then(data => ({ 
        data: data.map(parseObj) 
      }))
    },

    create: (_, params) => {       
      return dynamodb.putItem({
        attributes: formatObj(params.data),
        options: {
          ConditionExpression: `attribute_not_exists(#id)`,
          ExpressionAttributeNames:{
            '#id': keyNames[0]
          }
        },
      })
      .then(data => ({ data: parseObj(data) }))
    },
    delete: (_, params) => {
      return dynamodb.deleteItem({ 
        key: getKey(params.id)
      })
      .then(() => ({ data: params }))
    },
    deleteMany: (_, params) => {
      return dynamodb.batchDelete({ 
        keys: params.ids.map(getKey)
      })
      .then(() => ({ data: params.ids }))
    },
    update: (_, params) => {
      return dynamodb.putItem({
        attributes: formatObj(params.data),
      })
      .then(data => ({ data: parseObj(data) }))
    },    
    getList: async (_, params) => {
      const { pagination, filter } = params
      const { page, perPage } = pagination

      cache.splice(perPage * page - perPage)

      const limit = perPage * page - cache.length
      const startKey = cache.length > 0 && getKey(cache[cache.length-1])
      const {
        results,
        hasMore
      } = await (  // support filters when it's the primary key
        filter[keyNames[0]]
        ? dynamodb.query({ 
            keyName: keyNames[0],
            keyValue: filter[keyNames[0]],
            limit,
            startKey
          })
        : dynamodb.scan({ limit, startKey })
      )
      
      const parsedResults = results.map(parseObj)
      cache.push(...parsedResults.map(r => r.id))

      return {
        data: parsedResults.slice(perPage * -1),
        total: cache.length + (hasMore ? 1 : 0),
      }
    },

    getManyReference: (_, params) => {
      const { pagination, filter } = params
      const { page, perPage } = pagination

      cache.splice(perPage * page - perPage)

      const limit = perPage * page - cache.length
      const startKey = cache.length > 0 && getKey(cache[cache.length-1])

      return dynamodb.query({ 
        keyName: keyNames[0],
        keyValue: Array.isArray(params.id) ? params.id[0] : params.id, // this is temportary MAKE SURE TO REMOVE IT ONCE WE CAN LOG CHATS BY USER ID
        limit,
        startKey
      })
      .then(({ results, hasMore }) => {
        const parsedResults = results.map(parseObj)
        cache.push(...parsedResults.map(r => r.id))

        return {
          data: parsedResults.slice(perPage * -1),
          total: cache.length + (hasMore ? 1 : 0),
        }
      })      
    },
  }
}