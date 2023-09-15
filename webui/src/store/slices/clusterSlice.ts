import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";
import { ActionTypes, ClusterInfoType, ReplicasetType } from "./types";

export interface ClusterState {
  clusterInfo: ClusterInfoType;
  replicasets: ReplicasetType[];
}

const initialState: ClusterState = {
  clusterInfo: {
    capacityUsage: " ",
    memory: {
      used: "",
      usable: "",
    },
    replicasetsCount: 0,
    instancesCurrentGradeOnline: 0,
    instancesCurrentGradeOffline: 0,
    currentInstaceVersion: "",
  },
  replicasets: [
    {
      id: "",
      instanceCount: 0,
      instances: [
        {
          name: "",
          targetGrade: "",
          currentGrade: "",
          failureDomain: "",
          version: "",
          isLeader: false,
        },
      ],
      version: "",
      grade: "",
      capacity: "",
    },
  ],
};

export const getReplicasets = createAsyncThunk<ReplicasetType[]>(
  ActionTypes.getReplicasetsType,
  async () => {
    return new Promise((res) => {
      res([
        {
          id: "Test1231",
          instanceCount: 2,
          instances: [
            {
              name: "Test 1",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: true,
            },
            {
              name: "Test 2",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: false,
            },
          ],
          version: "1.1",
          grade: "Main grade",
          capacity: "2,5MiB/50 GiB",
        },
        {
          id: "Test0",
          instanceCount: 3,
          instances: [
            {
              name: "Best",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: false,
            },
            {
              name: "Lovely",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: true,
            },
          ],
          version: "1.1",
          grade: "Main grade",
          capacity: "2,5MiB/50 GiB",
        },
        {
          id: "Test123",
          instanceCount: 1,
          instances: [
            {
              name: "Donatello",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: true,
            },
            {
              name: "Leonardo",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: false,
            },
            {
              name: "Shredder",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: false,
            },
          ],
          version: "1.1",
          grade: "Main grade",
          capacity: "2,5MiB/50 GiB",
        },
        {
          id: "Test1232",
          instanceCount: 2,
          instances: [
            {
              name: "Raphael",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: false,
            },
            {
              name: "Michelangelo",
              targetGrade: "target grade",
              currentGrade: "current grade",
              failureDomain: "Failure",
              version: "1.2",
              isLeader: true,
            },
          ],
          version: "1.1",
          grade: "Main grade",
          capacity: "2,5MiB/50 GiB",
        },
      ]);
    });
  }
);

export const getClusterInfo = createAsyncThunk<ClusterInfoType>(
  ActionTypes.getClusterInfoType,
  async () => {
    return new Promise((res) => {
      res({
        capacityUsage: "30%",
        memory: {
          used: "3GB",
          usable: "24 GB",
        },
        replicasetsCount: 12,
        instancesCurrentGradeOnline: 12,
        instancesCurrentGradeOffline: 12,
        currentInstaceVersion: "1.22",
      });
    });
  }
);

export const clusterSlice = createSlice({
  name: "cluster",
  initialState,
  reducers: {},
  extraReducers: (builder) => {
    builder.addCase(getReplicasets.fulfilled, (state, action) => {
      return { ...state, replicasets: [...action.payload] };
    });
    builder.addCase(getClusterInfo.fulfilled, (state, action) => {
      return { ...state, clusterInfo: { ...action.payload } };
    });
  },
});

export default clusterSlice.reducer;
