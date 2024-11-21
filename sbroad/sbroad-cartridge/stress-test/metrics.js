import { Rate } from 'k6/metrics';
import tarantool from "k6/x/tarantool";

export let HOST = "localhost";
if (__ENV.HOST) {
  HOST = __ENV.HOST;
}

export const successRate = new Rate('success');

export function updateSuccessRate(resp) {
  if (resp.data[0][0] === null ) {
    successRate.add(false);
  } else {
    successRate.add(true);
  }
}

export function errorSuccessRate() {
  successRate.add(false);
}

export function tryTarantoolCall(client, func, args) {
  try {
      var resp = tarantool.call(client, func, args);
      return resp;
  } catch (e) {
      console.log(e);
      return null;
  }
}

export function callTarantool(client, func, args)  {
  var resp = tryTarantoolCall(client, func, args);
  if (resp == null) {
      errorSuccessRate();
  } else {
      updateSuccessRate(resp);
  }
}
