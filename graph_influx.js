const {
  InfluxDB,
  flux,
} = require('@influxdata/influxdb-client');

var express = require('express');

var { graphqlHTTP } = require('express-graphql');

var { buildSchema } = require('graphql');


// You can generate a Token from the "Tokens Tab" in the UI
const token = 'tokenrandon=='
const org = 'tenant1'
const influxClient = new InfluxDB({ url: 'http://localhost:8086', token: token })


// 1
const typeDefs = `
  #Value types for a future use in parsing data 
  enum ValueType {
     NUMBER
     STRING
     BOOLEAN
     GEO
  }

  #Necessary informations about device#
  input Device {
      id: String!
  }

  #Necessary data about and attribute of a device#
  input Attr {
     label: String!
     valueType: ValueType
  }

  input Range {
    start: String! #dateFrom#
    stop: String #dateTo#
  }

  #Parameters to query historical device data#
  input ParamsInput {
    #list of devices will be retrieved#
    devices: [Device]!
    #list of attributes will be retrieved#
    attributes: [Attr]
    # number of elements will be returned#
    limit: Int
    # time interval used in flux query
    range: Range
  }
  

  #main Query to request InfluxData
  type Query {
    #Returns historical data in the format used by the Dashboard
    getDeviceHistory(filter: ParamsInput): String

  }
`;

// Construct a schema, using GraphQL schema language
var schema = buildSchema(typeDefs);



const fluxFilter = (devices, attributes) => {
  const strDevices = devices.map((device) => {
    return `r._measurement == "${device.id}"`
  }).join(' or ');

  const strAttrs = attributes.map((attr) => {
    return `r._field == "${attr.label}"`
  }).join(' or ');

  let rtnString = `filter(fn: (r) => `;
  rtnString += `(${strDevices})`
  if (attributes.length)
    rtnString += ` and (${strAttrs})`
  rtnString += `))`
  return rtnString;
}

const fluxRange = (start, stop) => {
  // @todo should create validations for datetime?
  if (stop) {
    return `range(start: ${start}, stop: ${end})`
  }
  return `range(start: ${start})`
}

const fluxLimit = (limit) => {
  return `limit(${limit})`
}

const fluxYield = () => {
  return `yield()`
}

const closeInfluxConnection = (queryApi) => {
  queryApi
    .close()
    .then(() => {
      console.log('FINISHED')
    })
    .catch(e => {
      console.error(e)
      console.log('\\nFinished ERROR')
    })
};

// 2
//Query: {
const root = {
  async getDeviceHistory(
    root,
    params,
    context) {

    console.log("getDeviceHistory", root);
    const { filter:
      {
        range: { start = '', stop = '' },
        limit = 10,
        devices = [],
        attributes = [],
      }
    } = root;

    const fluxQuery = flux`from(bucket:"devices")
      |> ${fluxRange(start, stop)}
      |> ${fluxFilter(devices, attributes)}
      |> ${fluxLimit(limit)}
      |> ${fluxYield()}

      `;
    console.log("fluxQuery", fluxQuery.toString());


    const influxClient = new InfluxDB({
      url: 'http://localhost:8086', token: token
    })
    const queryApi = influxClient.getQueryApi({ org, gzip: false });

    // do query
    const rtn = await queryApi.queryRows(fluxQuery);
    console.log("rtn ", rtn);

    closeInfluxConnection(queryApi);

    return 'rtn';
  },
}

const app = express();

app.use('/graphql', graphqlHTTP({
  schema: schema,
  rootValue: root,
  graphiql: true,
}));

app.listen(4000);

console.log('Running a GraphQL API server at http://localhost:4000/graphql');
