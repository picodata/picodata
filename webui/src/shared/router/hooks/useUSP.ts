import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";
import { z } from "zod";

import { useMount } from "../../react/hooks/useMount";
import { useUnMount } from "../../react/hooks/useUnMount";

export interface USPOptions<T extends z.ZodSchema> {
  key: string;
  schema: T;
  defaultValue?: z.infer<T>;
  resetUnMount?: boolean;
}

export type USPReturn<T extends z.ZodSchema> = [
  value: z.TypeOf<T> | undefined,
  onChange: (value?: z.infer<T>) => void
];

export const useUSP = <T extends z.ZodSchema>(
  args: USPOptions<T>
): USPReturn<T> => {
  const [searchParams, setSearchParams] = useSearchParams();

  const urlValue = searchParams.get(args.key);

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
      const parsedValue = urlValue ? JSON.parse(urlValue) : urlValue;
      const validatedValue = args.schema.parse(parsedValue);

      return validatedValue as z.infer<T>;
    } catch (e) {
      return args.defaultValue;
    }
  }, [args.defaultValue, args.schema, urlValue]);

  useMount(() => {
    if (value != urlValue) {
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
