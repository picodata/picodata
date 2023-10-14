export const getStrokeColorByPercent = (percent: number) => {
  if (percent > 80) {
    return "#DD4A4A";
  }

  return "#6FFF9F";
};

export const getConfigByTheme = (theme: "primary" | "secondary") => {
  if (theme === "primary") {
    return {
      trailColor: "#F9F5F2",
    };
  }

  return {
    trailColor: "#FFFFFF",
  };
};

export const getConfigBySize = (size: "small" | "medium") => {
  if (size === "small") {
    return {
      height: 12,
    };
  }

  return {
    height: 24,
  };
};
