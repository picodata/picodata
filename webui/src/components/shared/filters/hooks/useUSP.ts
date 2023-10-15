/* eslint-disable no-restricted-imports */
import { useUnMount } from "../../react/hooks/useUnMount";
import { useMount } from "../../react/hooks/useMount";
import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { z } from "zod";

export const useUSP = <T extends z.ZodSchema>(args: {
  key: string;
  schema: T;
  defaultValue?: z.infer<T>;
  resetUnMount?: boolean;
}) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const selectUrlValue = searchParams.get(args.key);

  const onChange = useCallback(
    (value?: z.infer<T>) => {
      setSearchParams((params) => {
        if (value) params.set(args.key, String(value));

        if (value === undefined) params.delete(args.key);

        return params;
      });
    },
    [args.key, setSearchParams]
  );

  const value = useMemo(() => {
    try {
      const selectValue = args.schema.parse(selectUrlValue);

      return selectValue as z.infer<T>;
    } catch (e) {
      return args.defaultValue;
    }
  }, [args.defaultValue, args.schema, selectUrlValue]);

  useMount(() => {
    if (value != selectUrlValue) {
      onChange(value);
    }
  });

  useUnMount(() => {
    if (args.resetUnMount) {
      onChange();
    }
  });

  return [value, onChange] as const;
};
