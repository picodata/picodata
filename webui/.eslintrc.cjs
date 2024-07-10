module.exports = {
  extends: [
    "airbnb-typescript",
    "airbnb/hooks",
    "plugin:@typescript-eslint/recommended",
    "plugin:react/jsx-runtime",
    "plugin:import/recommended",
    "plugin:import/typescript",
    "prettier",
  ],
  plugins: ["react", "@typescript-eslint", "prettier", "import"],
  env: {
    browser: true,
    es6: true,
  },
  globals: {
    Atomics: "readonly",
    SharedArrayBuffer: "readonly",
  },
  ignorePatterns: [".eslintrc.cjs, vite.config.ts"],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: "module",
    project: ["./tsconfig.eslint.json"],
  },
  rules: {
    "prettier/prettier": "error",
    "import/prefer-default-export": "off",
    "no-use-before-define": "off",
    "react/prop-types": "off",
    "react/jsx-filename-extension": "off",
    "react/no-unescaped-entities": [
      "error",
      {
        forbid: [">", '"', "}"],
      },
    ],
    "no-plusplus": "off",
    "react/state-in-constructor": "off",
    "@typescript-eslint/no-use-before-define": ["error", { functions: false }],
    "@typescript-eslint/no-unused-vars": "error",
    "react/static-property-placement": "off",
    "import/no-named-as-default": "off",
    "@typescript-eslint/ban-ts-comment": [
      "error",
      {
        "ts-expect-error": "allow-with-description",
        "ts-ignore": "allow-with-description",
        "ts-nocheck": "allow-with-description",
        "ts-check": "allow-with-description",
      },
    ],
    "max-len": [0, 120, 2, { ignoreUrls: true }],
    "@typescript-eslint/dot-notation": ["warn"],
    "@typescript-eslint/no-explicit-any": [
      "error",
      { fixToUnknown: true, ignoreRestArgs: true },
    ],
    "no-console": ["error", { allow: ["error", "info", "warn"] }],
    "no-param-reassign": ["error", { props: false }],
    "react/destructuring-assignment": "off",
    "react/jsx-props-no-spreading": "off",
    "import/order": "off",
    "import/order": [
      2,
      {
        groups: [
          "builtin",
          "external",
          "internal",
          "parent",
          "sibling",
          "index",
        ],
        pathGroups: [
          {
            pattern: "*.scss",
            group: "index",
            patternOptions: { matchBase: true },
            position: "after",
          },
        ],
        pathGroupsExcludedImportTypes: [],
        "newlines-between": "always",
      },
    ],
    "import/prefer-default-export": "off",
    "import/no-duplicates": 2,
    "import/first": 2,
    // doesn't work with yup templates, example '${path} is required'
    "no-template-curly-in-string": "off",
    quotes: ["error", "double"],
  },
  settings: {
    "import/resolver": {
      node: {
        extensions: [".js", ".jsx", ".ts", ".tsx"],
        moduleDirectory: ["node_modules", "src/"],
        paths: ["src"],
      },
      typescript: {
        alwaysTryTypes: true,
      },
    },
    "import/ignore": [".mobule.css"],
  },
};
