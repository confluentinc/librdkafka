module.exports = {
    preset: 'ts-jest',
    testEnvironment: 'node',
    testMatch: ['**/test/**/*.ts', '**/e2e/**/*.ts'],
    transform: {
      '^.+\\.tsx?$': 'ts-jest',
    },
  };