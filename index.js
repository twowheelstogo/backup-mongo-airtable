const { MongoClient } = require('mongodb'); 
const { v4: uuidv4 } = require('uuid'); 
const fs = require('fs'); 
const readline = require('readline'); 
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const url = "mongodb://emanuel_admin:sAGqn1oghVVSpY4U@deliverycluster-shard-00-00.0og1c.mongodb.net:27017,deliverycluster-shard-00-01.0og1c.mongodb.net:27017,deliverycluster-shard-00-02.0og1c.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-o2fhya-shard-0&authSource=admin&retryWrites=true&w=majority"
const client = new MongoClient(url); 

//Database Name
const dbName = 'dbDeliveryCluster'; 
const date = new Date(); 
const year = date.getFullYear();
const month = date.getMonth(); 
const day = date.getDate(); 

async function main(){
    //use connect method to connect to the server 
    await client.connect();
    console.log('Connect successfully to server'); 
    const db = client.db(dbName); 
    const collection = db.collection('c_dashboard_exp'); 
    // let data = await collection.find({ FECHA_ENTREGA_CLEAN: { $gt: new Date(year-1, month, day-13), $lt: new Date(year, month, day)}});
    let data2 = await data.toArray();
    // const jsonArray = JSON.parse(JSON.stringify(data2));

    const csvWriter = createCsvWriter({
        path : `${uuidv4()}.csv`, 
        header : [
            {id: '_id' , title: '_id'},{id: 'CLIENTE' , title: 'CLIENTE'},{id: 'ALERTA' , title: 'ALERTA'},
            {id: 'AGENTE_CLIENTE' , title: 'AGENTE_CLIENTE'},{id: 'AGENTE' , title: 'AGENTE'},{id: 'EMPRESA' , title: 'EMPRESA'},
            {id: 'INTENTOS' , title: 'INTENTOS'},{id: 'CODIGO_PAIS' , title: 'CODIGO_PAIS'},{id: 'FECHA_ENTREGA_OBJ' , title: 'FECHA_ENTREGA_OBJ'},
            {id: 'DEPARTAMENTO' , title: 'DEPARTAMENTO'},{id: 'FECHA_CREACION_OBJ' , title: 'FECHA_CREACION_OBJ'},{id: 'CIRCULO' , title: 'CIRCULO'},
            {id: 'GEOLC_ENTREGADO' , title: 'GEOLC_ENTREGADO'},{id: 'GEOLC_REFERENCIA' , title: 'GEOLC_REFERENCIA'},{id: 'MUNICIPIO' , title: 'MUNICIPIO'},
            {id: 'NOTA' , title: 'NOTA'},{id: 'ID_ORDEN' , title: 'ID_ORDEN'},{id: 'FOTO_COMANDA' , title: 'FOTO_COMANDA'},
            {id: 'FOTO_RECIBIDO' , title: 'FOTO_RECIBIDO'},{id: 'TELEFONO' , title: 'TELEFONO'},{id: 'PIN' , title: 'PIN'},
            {id: 'ORIGEN' , title: 'ORIGEN'},{id: 'ESTADO' , title: 'ESTADO'},{id: 'TIEMPO_ENTREGA_COMPLETA' , title: 'TIEMPO_ENTREGA_COMPLETA'},
            {id: 'FECHA_ACTUALIZACION_OBJ' , title: 'FECHA_ACTUALIZACION_OBJ'},{id: 'MES' , title: 'MES'},{id: 'DIA_MES' , title: 'DIA_MES'},
            {id: 'HORA_ENTRADA_PEDIDO' , title: 'HORA_ENTRADA_PEDIDO'},{id: 'stateTime' , title: 'stateTime'},{id: 'FECHA_ENTREGA_CLEAN', title: 'FECHA_ENTREGA_CLEAN'},
            {id: 'FECHA_ENTREGA_MTH_YTD', title: 'FECHA_ENTREGA_MTH_YTD'},{id: 'FECHA_ENTREGA_YTD', title: 'FECHA_ENTREGA_YTD'},{id: 'FECHA_ENTREGA_MTH', title: 'FECHA_ENTREGA_MTH'},
            {id: 'geoloc' , title: 'geoloc'},
             
        ]
    }); 

    csvWriter 
        .writeRecords(data2)
        .then(() => console.log('Records Save with successfull'))
    return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  .finally(() => client.close());