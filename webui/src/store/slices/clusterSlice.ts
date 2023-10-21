import axios from "axios";
import { createAsyncThunk, createSlice } from "@reduxjs/toolkit";

import {
  ActionTypes,
  ClientInstanceType,
  ClientReplicasetType,
  ClusterInfoType,
  ReplicasetType,
} from "./types";

export interface ClusterState {
  clusterInfo?: ClusterInfoType;
  replicasets: ClientReplicasetType[];
  instances: ClientInstanceType[];
}

const initialState: ClusterState = {
  replicasets: [],
  instances: [],
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
      const replicasets = action.payload.map((replicaset) => {
        return {
          ...replicaset,
          instances: replicaset.instances.map((instance) => ({
            ...instance,
            failureDomain: Object.entries(instance.failureDomain).map(
              ([key, value]) => ({
                key,
                value,
              })
            ),
          })),
        };
      });

      return {
        ...state,
        replicasets,
        // todo нужен уникальный id для instances
        instances: replicasets
          .map((replicaset) => replicaset.instances)
          .flat(1),
      };
    });
    builder.addCase(getClusterInfo.fulfilled, (state, action) => {
      return { ...state, clusterInfo: { ...action.payload } };
    });
  },
});

export default clusterSlice.reducer;
