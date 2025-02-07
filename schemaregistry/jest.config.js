module.exports = {
    roots: [".."],
    transform: {
      '^.+\\.tsx?$': 'ts-jest',
    },
    "collectCoverage": true,
    "coverageReporters": ['json', 'text', 'html'],
    "coverageDirectory": 'coverage/jest/',
  };
