import { QueryClient, UseMutationOptions, UseQueryOptions } from "react-query";

export const queryClient = new QueryClient();

export const defaultQueryConfig = {
  refetchOnMount: false,
  refetchOnReconnect: true,
  refetchOnWindowFocus: true,
} satisfies UseQueryOptions | UseMutationOptions;
