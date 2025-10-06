export enum LoadingType {
  ABSOLUTE = "absolute",
  RELATIVE = "relative",
}

export interface LoadingIndicatorProps {
  size?: number;
  type?: LoadingType;
}
