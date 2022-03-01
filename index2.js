const fs = require('fs'); 
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const { v4: uuidv4 } = require('uuid'); 
const readline = require('readline');

async function proccessLineByLine() {
    const fileStream = await fs.createReadStream('_deliveries_firestore_87f6320b-d7cf-43ff-952c-03fc1e5fd617.jsonl'); 
    const csvWriter = createCsvWriter({
        path: `${uuidv4()}.csv`, 
        header: [
            {id: 'orderId' , title: 'orderId'},
            {id: 'creatorOrder' , title: 'creatorOrder'},
            {id: 'name' , title: 'name'},
            {id: 'time' , title: 'time'},
            {id: 'latBranch', title: 'latBranch'}, 
            {id: 'addressBranch', title: 'addressBranch'},
            {id: 'idBranch', title: 'idBranch'},
            {id: 'nameBranch', title: 'nameBranch'},
            {id: 'lagBranch', title: 'lagBranch'},
            {id: 'depto', title: 'depto'},
            {id: 'locationEndlat' , title: 'locationEndlat'},
            {id: 'locationEndlog' , title: 'locationEndlog'},
            {id: 'inRouteSeconds', title: 'inRouteSeconds'},
            {id: 'inRouteNano', title: 'inRouteNano'},
            {id: 'acceptedSeconds', title: 'acceptedSeconds'},
            {id: 'acceptedNano', title: 'acceptedNano'},
            {id: 'scheduledSeconds', title: 'scheduledSeconds'},
            {id: 'scheduledNano', title: 'scheduledNano'},
            {id: 'deliveredSeconds', title: 'deliveredSeconds'},
            {id: 'deliveredNano', title: 'deliveredNano'},
            {id: 'inLineSeconds', title: 'inLineSeconds'},
            {id: 'inLineNano', title: 'inLineNano'},
            {id: 'assignedSeconds', title: 'assignedSeconds'},
            {id: 'assignedNano', title: 'assignedNano'},
            {id: 'receivingSeconds', title: 'receivingSeconds'},
            {id: 'receivingNano', title: 'receivingNano'},
            {id: 'calulatedCircle' , title: 'calulatedCircle'},
            {id: 'timeCircle' , title: 'timeCircle'},
            {id: 'idCircle' , title: 'idCircle'},
            {id: 'distanceCircle' , title: 'distanceCircle'},
            {id: 'deliveryDateSeconds' , title: 'deliveryDateSeconds'},
            {id: 'deliveryDateNanoSeconds' , title: 'deliveryDateNanoSeconds'},
            {id: 'alert' , title: 'alert'},
            {id: 'idAlias' , title: 'idAlias'},
            {id: 'nameAlias' , title: 'nameAlias'},
            {id: 'deliveredAtSeconds' , title: 'deliveredAtSeconds'},
            {id: 'deliveredAtNanoSeconds' , title: 'deliveredAtNanoSeconds'},
            {id: 'nota' , title: 'nota'},
            {id: 'companyId' , title: 'companyId'},
            {id: 'companyName' , title: 'companyName'},
            {id: 'payMethod' , title: 'payMethod'},
            {id: 'phone' , title: 'phone'},
            {id: 'address' , title: 'address'},
            {id: 'status' , title: 'status'},
            {id: 'receivedPic' , title: 'receivedPic'},
            {id: 'isPriority' , title: 'isPriority'},
            {id: 'isActive' , title: 'isActive'},
            {id: 'toBeDeliveredAtSeconds' , title: 'toBeDeliveredAtSeconds'},
            {id: 'toBeDeliveredAtNano' , title: 'toBeDeliveredAtNano'},
            {id: 'modifyName' , title: 'modifyName'},
            {id: 'modifyid' , title: 'modifyid'},
            {id: 'modifyexpriesAtnano' , title: 'modifyexpriesAtnano'},
            {id: 'modifyexpriesAtseconds', title: 'modifyexpriesAtseconds'},
            {id: 'locationReflat' , title: 'locationReflat'},
            {id: 'locationReflon' , title: 'locationReflon'},
            {id: 'createdAtSeconds' , title: 'createdAtSeconds'},
            {id: 'createdAtNano' , title: 'createdAtNano'},
            {id: 'zone' , title: 'zone'},
            {id: 'provider' , title: 'provider'},
            {id: 'orderPic', title: 'orderPic'},
            {id: 'creatorId' , title: 'creatorId'},
            {id: 'typeOrder' , title: 'typeOrder'},
            {id: 'distanceTraveled', title: 'distanceTraveled'},
            {id: 'updatedAtSeconds', title: 'updatedAtSeconds'},
            {id: 'updatedAtNano', title: 'updatedAtNano'},
            {id: 'id', title: 'id'}
        ]    
    }); 
    
    const rl = readline.createInterface({
        input: fileStream, 
        crlfDelay: Infinity
    })

    let count = 0; 
    for await (const ln of rl){
        count++;
        const data = [JSON.parse(ln)];
        console.log(count);
        data.map(function(obj){
            console.log(obj)
            obj.latBranch = obj.branch?.lat || "";
            obj.addressBranch = obj.branch?.addres || ""; 
            obj.idBranch = obj.branch?.id || "No tiene"; 
            obj.nameBranch = obj.branch?.name || "No tiene";
            obj.lagBranch = obj.branch?.lag || "No tiene"; 
            obj.inRouteStatetime = obj.stateTime['En ruta'] || "No tiene"; 
            obj.inRouteSeconds = obj.inRouteStatetime?._seconds || "No tiene";
            obj.inRouteNano = obj.inRouteStatetime?._nanoseconds || "No tiene";
            obj.acceptedStatetime = obj.stateTime['Aceptado'] || "No tiene"; 
            obj.acceptedSeconds = obj.acceptedStatetime?._seconds || "No tiene";
            obj.acceptedNano = obj.acceptedStatetime?._nanoseconds || "No tiene";
            obj.scheduledStatetime = obj.stateTime['Reagendado'] || "No tiene";
            obj.scheduledSeconds = obj.scheduledStatetime?._seconds || "No tiene";
            obj.scheduledNano = obj.scheduledStatetime?._nanoseconds || "No tiene";
            obj.deliveredStatetime = obj.stateTime['Entregado'] || "No tiene";
            obj.deliveredSeconds = obj.deliveredStatetime?._seconds || "No tiene";
            obj.deliveredNano = obj.deliveredStatetime?._nanoseconds || "No tiene";
            obj.inLineStatetime = obj.stateTime['En cola'] || "No tiene";
            obj.inLineSeconds = obj.inLineStatetime?._seconds || "No tiene";
            obj.inLineNano = obj.inLineStatetime?._nanoseconds || "No tiene";
            obj.assignedStatetime = obj.stateTime['Asignado'] || "No tiene";
            obj.assignedSeconds = obj.assignedStatetime?._seconds || "No tiene";
            obj.assignedNano = obj.assignedStatetime?._nanoseconds || "No tiene";
            obj.receivingStatetime = obj.stateTime['Recibiendo'] || "No tiene";
            obj.receivingSeconds = obj.receivingStatetime?._seconds || "No tiene";
            obj.receivingNano = obj.receivingStatetime?._nanoseconds || "No tiene";
            obj.locationEndlat = obj.locationEnd?._latitude || "No tiene";
            obj.locationEndlog = obj.locationEnd?._longitude  || "No tiene";
            obj.calulatedCircle = obj.circle?.calulated  || "false";
            obj.timeCircle = obj.circle?.time  || "No tiene";
            obj.idCircle = obj.circle?.id  || "No tiene";
            obj.distanceCircle = obj.circle?.distance  || "No tiene";
            obj.deliveryDateSeconds = obj.deliveryDate?._seconds || "No tiene";
            obj.deliveryDateNanoSeconds = obj.deliveryDate?._nanoseconds || "0";
            obj.idAlias = obj.alias?.id || "No tiene";
            obj.nameAlias = obj.alias?.name || "No tiene";
            obj.deliveredAtSeconds = obj.deliveredAt?._seconds  || "No tiene";
            obj.deliveredAtNanoSeconds = obj.deliveredAt?._nanoseconds || "No tiene";
            obj.companyId = obj.company?.id || "No tiene";
            obj.companyName = obj.company?.name || "No tiene";
            obj.toBeDeliveredAtSeconds = obj.toBeDeliveredAt?._seconds || "No tiene";
            obj.toBeDeliveredAtNano = obj.toBeDeliveredAt?._nanoseconds || "No tiene";
            obj.modifyName = obj.modifyingBy?.name || "No tiene";
            obj.modifyid = obj.modifyingBy?.id  || "No tiene";
            obj.modifyexpriesAt = obj.modifyingBy?.expiresAt || "No tiene";
            obj.modifyexpriesAtnano = obj.modifyexpriesAt?.nanoseconds || "No tiene";
            obj.modifyexpriesAtseconds = obj.modifyexpriesAt?.seconds || "No tiene";
            obj.locationReflat = obj.locationRef?._latitude || "No tiene";
            obj.locationReflon = obj.locationRef?._longitude || "No tiene";
            obj.createdAtSeconds = obj.createdAt?._seconds || "No tiene";
            obj.createdAtNano = obj.createdAt?._nanoseconds || "No tiene";
            obj.updatedAtSeconds = obj.updatedAt?._seconds || "No tiene";
            obj.updatedAtNano = obj.updatedAt?._nanoseconds || "No tiene";
            delete obj.branch;
            delete obj.stateTime; 
            delete obj.locationEnd; 
            delete obj.circle;
            delete obj.deliveryDate; 
            delete obj.alias;
            delete obj.deliveredAt;
            delete obj.company;
            delete obj.toBeDeliveredAt;
            delete obj.modifyingBy;
            delete obj.modifyingBy;
            delete obj.locationRef;
            delete obj.createdAt;
            delete obj.updatedAt;
        });

        console.log(data);

        csvWriter 
        .writeRecords(data)
        .then(() => console.log('Records Save with successfull'))
    }

}

proccessLineByLine(); 
