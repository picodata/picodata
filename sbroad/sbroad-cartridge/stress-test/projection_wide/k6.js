import tarantool from "k6/x/tarantool";
import {randomItem} from 'https://jslib.k6.io/k6-utils/1.1.0/index.js';
import { callTarantool, HOST } from '../metrics.js';

const clients = [
    tarantool.connect([HOST + ":3301"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([HOST + ":3306"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([HOST + ":3307"], {"user": "admin", pass: "app-cluster-cookie"}),
    tarantool.connect([HOST + ":3308"], {"user": "admin", pass: "app-cluster-cookie"})
]

export let current_server = 0

function get_client() {
    let c = clients[current_server]
    current_server += 1
    if (current_server >= clients.length) {
        current_server = 0
    }
    return c
}

let ids = Array.from(
    {
        length: 1000000
    },
    (_, id) => id
)

const pattern = `SELECT *
    FROM (SELECT "vehicleguid", "reestrid", "reestrstatus", "vehicleregno", "vehiclevin", "vehiclevin2", "vehiclechassisnum", "vehiclereleaseyear", "operationregdoctypename", "operationregdoc", "operationregdocissuedate", "operationregdoccomments", "vehicleptstypename", "vehicleptsnum", "vehicleptsissuedate", "vehicleptsissuer", "vehicleptscomments", "vehiclebodycolor", "vehiclebrand", "vehiclemodel", "vehiclebrandmodel", "vehiclebodynum", "vehiclecost", "vehiclegasequip", "vehicleproducername", "vehiclegrossmass", "vehiclemass", "vehiclesteeringwheeltypeid", "vehiclekpptype", "vehicletransmissiontype", "vehicletypename", "vehiclecategory", "vehicletypeunit", "vehicleecoclass", "vehiclespecfuncname", "vehicleenclosedvolume", "vehicleenginemodel", "vehicleenginenum", "vehicleenginepower", "vehicleenginepowerkw", "vehicleenginetype", "holdrestrictiondate", "approvalnum", "approvaldate", "approvaltype", "utilizationfeename", "customsdoc", "customsdocdate", "customsdocissue", "customsdocrestriction", "customscountryremovalid", "customscountryremovalname", "ownerorgname", "ownerinn", "ownerogrn", "ownerkpp", "ownerpersonlastname", "ownerpersonfirstname", "ownerpersonmiddlename", "ownerpersonbirthdate", "ownerbirthplace", "ownerpersonogrnip", "owneraddressindex", "owneraddressmundistrict", "owneraddresssettlement", "owneraddressstreet", "ownerpersoninn", "ownerpersondoccode", "ownerpersondocnum", "ownerpersondocdate", "operationname", "operationdate", "operationdepartmentname", "operationattorney", "operationlising", "holdertypeid", "holderpersondoccode", "holderpersondocnum", "holderpersondocdate", "holderpersondocissuer", "holderpersonlastname", "holderpersonfirstname", "holderpersonmiddlename", "holderpersonbirthdate", "holderpersonbirthregionid", "holderpersonsex", "holderpersonbirthplace", "holderpersoninn", "holderpersonsnils", "holderpersonogrnip", "holderaddressguid", "holderaddressregionid", "holderaddressregionname", "holderaddressdistrict", "holderaddressmundistrict", "holderaddresssettlement", "holderaddressstreet", "holderaddressbuilding", "holderaddressstructureid", "holderaddressstructurename", "holderaddressstructure"
    FROM "vehicle_history"
    WHERE "sys_from" <= 332 AND "sys_to" >= 332
    UNION ALL
    SELECT "vehicleguid", "reestrid", "reestrstatus", "vehicleregno", "vehiclevin", "vehiclevin2", "vehiclechassisnum", "vehiclereleaseyear", "operationregdoctypename", "operationregdoc", "operationregdocissuedate", "operationregdoccomments", "vehicleptstypename", "vehicleptsnum", "vehicleptsissuedate", "vehicleptsissuer", "vehicleptscomments", "vehiclebodycolor", "vehiclebrand", "vehiclemodel", "vehiclebrandmodel", "vehiclebodynum", "vehiclecost", "vehiclegasequip", "vehicleproducername", "vehiclegrossmass", "vehiclemass", "vehiclesteeringwheeltypeid", "vehiclekpptype", "vehicletransmissiontype", "vehicletypename", "vehiclecategory", "vehicletypeunit", "vehicleecoclass", "vehiclespecfuncname", "vehicleenclosedvolume", "vehicleenginemodel", "vehicleenginenum", "vehicleenginepower", "vehicleenginepowerkw", "vehicleenginetype", "holdrestrictiondate", "approvalnum", "approvaldate", "approvaltype", "utilizationfeename", "customsdoc", "customsdocdate", "customsdocissue", "customsdocrestriction", "customscountryremovalid", "customscountryremovalname", "ownerorgname", "ownerinn", "ownerogrn", "ownerkpp", "ownerpersonlastname", "ownerpersonfirstname", "ownerpersonmiddlename", "ownerpersonbirthdate", "ownerbirthplace", "ownerpersonogrnip", "owneraddressindex", "owneraddressmundistrict", "owneraddresssettlement", "owneraddressstreet", "ownerpersoninn", "ownerpersondoccode", "ownerpersondocnum", "ownerpersondocdate", "operationname", "operationdate", "operationdepartmentname", "operationattorney", "operationlising", "holdertypeid", "holderpersondoccode", "holderpersondocnum", "holderpersondocdate", "holderpersondocissuer", "holderpersonlastname", "holderpersonfirstname", "holderpersonmiddlename", "holderpersonbirthdate", "holderpersonbirthregionid", "holderpersonsex", "holderpersonbirthplace", "holderpersoninn", "holderpersonsnils", "holderpersonogrnip", "holderaddressguid", "holderaddressregionid", "holderaddressregionname", "holderaddressdistrict", "holderaddressmundistrict", "holderaddresssettlement", "holderaddressstreet", "holderaddressbuilding", "holderaddressstructureid", "holderaddressstructurename", "holderaddressstructure"
    FROM "vehicle_actual"
    WHERE "sys_from" <= 332) AS "t3"
    WHERE "reestrid" = ?`

export default () => {
    callTarantool(get_client(), "sbroad.execute", [pattern, [randomItem(ids)]]);
}
