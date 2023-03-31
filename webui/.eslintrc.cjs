module.exports = {
  extends: [
    'airbnb-typescript',
    'airbnb/hooks',
    'plugin:@typescript-eslint/recommended',
    'plugin:react/jsx-runtime',
    'plugin:import/recommended',
    'plugin:import/typescript',
    'prettier',
  ],
  plugins: ['react', '@typescript-eslint', 'prettier', 'import'],
  env: {
    browser: true,
    es6: true,
  },
  globals: {
    Atomics: 'readonly',
    SharedArrayBuffer: 'readonly',
  },
  ignorePatterns: ['.eslintrc.cjs, vite.config.ts'],
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module',
    project: ['./tsconfig.eslint.json'],
  },
  rules: {
    'prettier/prettier': 'error',
    'import/prefer-default-export': 'off',
    'no-use-before-define': 'off',
    'react/prop-types': 'off',
    'react/jsx-filename-extension': 'off',
    'react/no-unescaped-entities': [
      'error',
      {
        forbid: ['>', '"', '}'],
      },
    ],
    'no-plusplus': 'off',
    'react/state-in-constructor': 'off',
    '@typescript-eslint/no-use-before-define': ['error', { functions: false }],
    '@typescript-eslint/no-unused-vars': 'error',
    'react/static-property-placement': 'off',
    'import/no-named-as-default': 'off',
    '@typescript-eslint/ban-ts-comment': [
      'error',
      {
        'ts-expect-error': 'allow-with-description',
        'ts-ignore': 'allow-with-description',
        'ts-nocheck': 'allow-with-description',
        'ts-check': 'allow-with-description',
      },
    ],
    // 'import/no-extraneous-dependencies': [
    //     'error',
    //     {
    //         devDependencies: [
    //             'src/shared/utils/test.util.tsx',
    //             'src/setupTests.ts',
    //             'src/mocks/**/*',
    //             '**/*.stories.tsx',
    //             '**/*.test.tsx',
    //         ],
    //     },
    // ],
    'max-len': [0, 120, 2, { ignoreUrls: true }],
    '@typescript-eslint/dot-notation': ['warn'],
    '@typescript-eslint/no-explicit-any': ['error', { fixToUnknown: true, ignoreRestArgs: true }],
    'no-console': ['error', { allow: ['error', 'info', 'warn'] }],
    'no-param-reassign': ['error', { props: false }],
    'react/destructuring-assignment': 'off',
    'react/jsx-props-no-spreading': 'off',
    'import/order': 'off',
    // doesn't work with yup templates, example '${path} is required'
    'no-template-curly-in-string': 'off',
    'no-restricted-imports': [
      'error',
      {
        patterns: [{ group: ['../../*'], message: 'usage of relative imports by more than one level not allowed' }],
      },
    ],
  },
  settings: {
    'import/resolver': {
      node: {
        extensions: ['.js', '.jsx', '.ts', '.tsx'],
        moduleDirectory: ['node_modules', 'src/'],
      },
      typescript: {
        alwaysTryTypes: true,
      },
    },
  },
};
