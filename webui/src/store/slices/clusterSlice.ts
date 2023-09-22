import axios from "axios";
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
      used: 0,
      usable: 0,
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

// TODO: Research how to add base URL to dev builds
const GET_REPLICASETS_URL = "/api/v1/replicaset";

export const getReplicasets = createAsyncThunk<ReplicasetType[]>(
  ActionTypes.getReplicasetsType,
  async () => {
    const response = await axios.get(GET_REPLICASETS_URL);
    return response.data;
  }
);

// TODO: Research how to add base URL to dev builds
const CLUSTER_INFO_URL = "/api/v1/cluster";

export const getClusterInfo = createAsyncThunk<ClusterInfoType>(
  ActionTypes.getClusterInfoType,
  async () => {
    const response = await axios.get(CLUSTER_INFO_URL);
    return response.data;
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
