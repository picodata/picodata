import { ServerTiersListType } from "./types";

export const mock: ServerTiersListType = [
  {
    replicasets: [
      {
        version: "??.??",
        state: "Online",
        instanceCount: 2,
        capacityUsage: 100,
        instances: [
          {
            version: "??.??",
            failureDomain: {
              HOST: "2",
              DC: "1",
            },
            isLeader: true,
            currentState: "Online",
            targetState: "Online",
            name: "i2",
            binaryAddress: "127.0.0.1:3402",
          },
          {
            version: "??.??",
            failureDomain: {
              HOST: "1",
              DC: "2",
            },
            isLeader: false,
            currentState: "Online",
            targetState: "Online",
            name: "i4",
            binaryAddress: "127.0.0.1:3403",
          },
        ],
        memory: {
          usable: 33554432,
          used: 33554432,
        },
        id: "r2",
      },
      {
        version: "??.??",
        state: "Online",
        instanceCount: 2,
        capacityUsage: 100,
        instances: [
          {
            httpAddress: "127.0.0.1:8080",
            version: "??.??",
            failureDomain: {
              HOST: "1",
              DC: "1",
            },
            isLeader: false,
            currentState: "Online",
            targetState: "Online",
            name: "i1",
            binaryAddress: "127.0.0.1:3401",
          },
          {
            version: "??.??",
            failureDomain: {
              HOST: "2",
              DC: "2",
            },
            isLeader: true,
            currentState: "Online",
            targetState: "Online",
            name: "i5",
            binaryAddress: "127.0.0.1:3404",
          },
        ],
        memory: {
          usable: 33554432,
          used: 33554432,
        },
        id: "r1",
      },
    ],
    replicasetCount: 2,
    rf: 2,
    instanceCount: 4,
    can_vote: true,
    name: "red",
    plugins: [],
  },
  {
    replicasets: [
      {
        version: "??.??",
        state: "Online",
        instanceCount: 1,
        capacityUsage: 100,
        instances: [
          {
            version: "??.??",
            failureDomain: {
              HOST: "3",
              DC: "2",
            },
            isLeader: true,
            currentState: "Online",
            targetState: "Online",
            name: "i6",
            binaryAddress: "127.0.0.1:3406",
          },
        ],
        memory: {
          usable: 33554432,
          used: 33554432,
        },
        id: "r4",
      },
      {
        version: "??.??",
        state: "Online",
        instanceCount: 1,
        capacityUsage: 100,
        instances: [
          {
            version: "??.??",
            failureDomain: {
              HOST: "3",
              DC: "1",
            },
            isLeader: true,
            currentState: "Online",
            targetState: "Online",
            name: "i3",
            binaryAddress: "127.0.0.1:3405",
          },
        ],
        memory: {
          usable: 33554432,
          used: 33554432,
        },
        id: "r3",
      },
    ],
    replicasetCount: 2,
    rf: 1,
    instanceCount: 2,
    can_vote: true,
    name: "blue",
    plugins: [],
  },
];
