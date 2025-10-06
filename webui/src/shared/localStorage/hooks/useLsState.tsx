import { useCallback, useState } from "react";
import { z } from "zod";

export interface LsStateOptions<T extends z.ZodSchema> {
  key: string;
  schema: T;
  defaultValue: z.infer<T>;
}

export const useLsState = <T extends z.ZodSchema>(args: LsStateOptions<T>) => {
  const [value, setValue] = useState(() => {
    try {
      return getLsValue<T>(args);
    } catch (e) {
      return args.defaultValue;
    }
  });

  const onChange = useCallback(
    (newValue?: z.infer<T>) => {
      setValue(newValue);
      localStorage.setItem(args.key, JSON.stringify(newValue));
    },
    [args.key, setValue]
  );

  return [value, onChange] as const;
};

export function getLsValue<T extends z.ZodSchema>(args: LsStateOptions<T>) {
  const lsValue = localStorage.getItem(args.key);
  const parsedValue = lsValue ? JSON.parse(lsValue) : args.defaultValue;
  const validatedValue = args.schema.parse(parsedValue);

  return validatedValue as z.infer<T>;
}
