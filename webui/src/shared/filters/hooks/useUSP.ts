/* eslint-disable no-restricted-imports */
import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { z } from "zod";

import { useMount } from "../../react/hooks/useMount";
import { useUnMount } from "../../react/hooks/useUnMount";

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
        if (value) params.set(args.key, JSON.stringify(value));

        if (value === undefined) params.delete(args.key);

        return params;
      });
    },
    [args.key, setSearchParams]
  );

  const value = useMemo(() => {
    try {
      const parsedValue = selectUrlValue
        ? JSON.parse(selectUrlValue)
        : selectUrlValue;
      const selectValue = args.schema.parse(parsedValue);

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
