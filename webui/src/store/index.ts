import { configureStore } from "@reduxjs/toolkit";

import { clusterSlice } from "./slices/clusterSlice";

export const store = configureStore({
  reducer: {
    cluster: clusterSlice.reducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;
