module.exports = {
    "env": {
        "browser": true,
        "commonjs": true,
        "es2021": true
    },
    "extends": "eslint:recommended",
    "overrides": [
        {
            "env": {
                "node": true
            },
            "files": [
                ".eslintrc.{js,cjs}"
            ],
            "parserOptions": {
                "sourceType": "script"
            }
        },
        {
            "files": ["*.ts"],
            "parser": "@typescript-eslint/parser",
            "parserOptions": {
              "ecmaVersion": 2020,
              "sourceType": "module"
            },
            "extends": [
              "plugin:@typescript-eslint/recommended",
            ]
          }
    ],
    "parserOptions": {
        "ecmaVersion": "latest"
    },
    "rules": {
    }
}
